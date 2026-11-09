package org.dozendb.backup

import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.store.CommittedTransaction
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.PhysicalFlushableChannel
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.BinarySupportedKernelVersions
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.kernel.impl.transaction.log.LogIndexEncoding
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.storageengine.api.StorageEngineFactory

/** Restores stores from the backups of a [PhasedSource]. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StorePreparerTest {
    private lateinit var root: Path
    private val fs = DefaultFileSystemAbstraction()
    private lateinit var scheduler: JobScheduler
    private lateinit var pageCache: PageCache

    private lateinit var source: PhasedSource

    @BeforeAll
    fun writeSource(
        // Kept when a test fails, for the servers' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        this.root = root
        scheduler = JobSchedulerFactory.createInitialisedScheduler()
        pageCache = StandalonePageCacheFactory.createPageCache(fs, scheduler, PageCacheTracer.NULL)
        source = PhasedSource.write(root)
    }

    @AfterAll
    fun close() {
        pageCache.close()
        scheduler.close()
    }

    private fun state(server: CommunityServer) = PhasedSource.state(server)

    private fun target(name: String): DatabaseLayout =
        Neo4jLayout.of(config(name)).databaseLayout(CommunityServer.DATABASE)

    private fun config(name: String): Config =
        Config.defaults(GraphDatabaseSettings.neo4j_home, root.resolve(name).toAbsolutePath())

    private fun preparer(name: String) =
        StorePreparer(fs, pageCache, config(name), NullLogProvider.getInstance())

    private fun restored(name: String): Map<String, Any?> {
        val server = CommunityServer(root.resolve(name)).start()
        server.use {
            val state = state(server)
            server.stop()
            assertThat(server.consistent()).describedAs("$name consistent").isTrue
            return state
        }
    }

    private fun chain(vararg files: Path, until: RestorePoint? = null): ArtifactChain {
        val found = ArtifactChains.find(files.toList(), fs)
        assertThat(found.unreadable).isEmpty()
        return ArtifactChains.select(found.artifacts, until)
    }

    @Test
    fun `restores a dump`() {
        val prepared =
            preparer("dump").prepare(source.dump, emptyList(), null, target("dump"), detach = true)

        assertThat(prepared.restored).isZero
        assertThat(prepared.previousConsensusIndex).isNull()
        assertThat(restored("dump")).isEqualTo(source.states[0])
    }

    @Test
    fun `restores a chain to its last transaction`() {
        val chain = chain(source.backups)
        assertThat(chain.artifacts.map { it.path })
            .containsExactly(source.full, source.first, source.second)

        val prepared = preparer("latest").prepare(chain, null, target("latest"), detach = true)

        assertThat(prepared.lastTransaction.id()).isEqualTo(source.lastTransactionId)
        assertThat(prepared.restored)
            .isEqualTo(
                chain.differentials.sumOf {
                    it.header.highestAppendIndex - it.header.lowestAppendIndex + 1
                }
            )
        assertThat(restored("latest")).isEqualTo(source.states[2])
    }

    @Test
    fun `restores up to a transaction`() {
        val until = RestorePoint.BeforeTransaction(source.phaseStarts[1].transactionId)
        val chain = chain(source.backups, until = until)
        assertThat(chain.artifacts.map { it.path })
            .containsExactly(source.full, source.first, source.second)

        val prepared =
            preparer("transaction").prepare(chain, until, target("transaction"), detach = true)

        assertThat(prepared.lastTransaction.id()).isEqualTo(source.phaseStarts[1].transactionId - 1)
        assertThat(restored("transaction")).isEqualTo(source.states[1])
    }

    @Test
    fun `restores up to a time`() {
        val until =
            RestorePoint.BeforeTime(Instant.ofEpochMilli(source.phaseStarts[1].batch.timeCommitted))

        preparer("time")
            .prepare(chain(source.backups, until = until), until, target("time"), detach = true)

        assertThat(restored("time")).isEqualTo(source.states[1])
    }

    @Test
    fun `refuses a chain with a missing link or a full backup after the point`() {
        val firstHeader = fs.openAsInputStream(source.first).use(Artifacts::readHeader)

        assertThatThrownBy { chain(source.full, source.second) }
            .isInstanceOf(IOException::class.java)
            .hasMessage(
                "no chain of backups leads to ${source.second.fileName}: no backup of neo4j ends at " +
                    "transaction ${firstHeader.highestAppendIndex}, which ${source.second.fileName} continues"
            )
        assertThatThrownBy { chain(source.backups, until = RestorePoint.BeforeTransaction(2)) }
            .hasMessage("no full backup of neo4j holds only transactions before transaction 2")
        assertThatThrownBy {
                preparer("differential")
                    .prepare(source.first, emptyList(), null, target("differential"), detach = true)
            }
            .hasMessage(
                "${source.first} is a differential backup: restore it with the full backup it continues"
            )
    }

    @Test
    fun `a store from a Raft group gets an empty transaction without a Raft index after it`() {
        // The first differential backup's transactions, as replicated ones with Raft indexes.
        val readers = StorageEngineFactory.defaultStorageEngine().commandReaderFactory()
        val transactions =
            fs.openAsInputStream(source.first).use { input ->
                Artifacts.readDifferential(input, readers) { _, transactions ->
                    transactions.toList()
                }
            }
        val replicated = withRaftIndexes(transactions, firstIndex = 7000)
        val header = fs.openAsInputStream(source.first).use(Artifacts::readHeader)
        val artifact =
            root.resolve("replicated").also(Files::createDirectories).resolve("neo4j-2-raft.backup")
        Artifacts.writeDifferential(
            Files.newOutputStream(artifact),
            header.copy(lastChecksum = replicated.last().checksum),
            replicated.asSequence(),
            root.resolve("replicated/staging"),
            fs,
        )

        val prepared =
            preparer("raft")
                .prepare(chain(source.full, artifact), null, target("raft"), detach = true)

        assertThat(prepared.previousConsensusIndex).isEqualTo(7000L + replicated.size - 1)
        assertThat(prepared.lastTransaction.consensusIndex()).isEqualTo(-1)
        assertThat(prepared.lastTransaction.id()).isEqualTo(header.highestAppendIndex + 1)
        assertThat(prepared.lastRestored.id()).isEqualTo(header.highestAppendIndex)
        assertThat(restored("raft")).isEqualTo(source.states[1])
        // As for an aggregated backup, which the next differential backup must continue.
        val kept =
            preparer("kept")
                .prepare(chain(source.full, artifact), null, target("kept"), detach = false)
        assertThat(kept.previousConsensusIndex).isNull()
        assertThat(kept.lastTransaction.consensusIndex()).isEqualTo(7000L + replicated.size - 1)
        assertThat(kept.lastTransaction.checksum()).isEqualTo(replicated.last().checksum)
    }

    /** [transactions] with Raft indexes from [firstIndex] on, and the checksums those give. */
    private fun withRaftIndexes(
        transactions: List<CommittedTransaction>,
        firstIndex: Long,
    ): List<CommittedTransaction> =
        PhysicalFlushableChannel(fs.write(root.resolve("checksums")), EmptyMemoryTracker.INSTANCE)
            .use { channel ->
                val writer =
                    LogEntryWriter(channel, BinarySupportedKernelVersions(Config.defaults()))
                var previous = transactions.first().previousChecksum
                transactions.mapIndexed { index, transaction ->
                    val batch = transaction.batch
                    batch.setConsensusIndex(firstIndex + index)
                    val version = batch.kernelVersion()
                    writer.writeStartEntry(
                        version,
                        batch.timeStarted,
                        batch.latestCommittedTxWhenStarted,
                        transaction.appendIndex,
                        previous,
                        LogIndexEncoding.encodeLogIndex(batch.consensusIndex()),
                    )
                    writer.serialize(batch, version)
                    val checksum =
                        writer.writeCommitEntry(
                            version,
                            transaction.transactionId,
                            batch.timeCommitted,
                        )
                    CommittedTransaction(
                            transaction.transactionId,
                            transaction.appendIndex,
                            previous,
                            checksum,
                            batch,
                        )
                        .also { previous = checksum }
                }
            }
}
