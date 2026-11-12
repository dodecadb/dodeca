package org.dozendb.backup.source

import java.nio.file.Path
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.CommunityServer
import org.dozendb.core.store.CommittedTransaction
import org.dozendb.core.store.CommittedTransactions
import org.dozendb.core.store.CommittedTransactions.Behind
import org.dozendb.core.store.CommittedTransactions.Continued
import org.dozendb.core.store.CommittedTransactions.Diverged
import org.dozendb.core.store.CommittedTransactions.Pruned
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.config.Setting
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.database.Database
import org.neo4j.storageengine.AppendIndexProvider
import org.neo4j.storageengine.api.TransactionIdStore
import org.neo4j.wal.checkpoint.CheckPointer
import org.neo4j.wal.checkpoint.SimpleTriggerInfo

class CommittedTransactionsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    private fun CommunityServer.databaseInternal(): Database =
        database().dependencyResolver.resolveDependency(Database::class.java)

    private fun CommunityServer.checkPointer(): CheckPointer =
        database().dependencyResolver.resolveDependency(CheckPointer::class.java)

    private fun CommunityServer.checksumAt(appendIndex: Long): Int =
        transactionsAfter(appendIndex - 1).first().checksum

    private fun fields(transactions: List<CommittedTransaction>) = transactions.map {
        listOf(it.transactionId, it.appendIndex, it.previousChecksum, it.checksum)
    }

    @Test
    fun `reads the transactions after a backup's last one, up to the database's last`() {
        CommunityServer(root).start().use { server ->
            server.database().executeTransactionally("CREATE (:Before)")
            val after = server.lastCommitted()
            CommunityServer.populate(server)
            val expected = server.transactionsAfter(after.appendIndex())

            val read =
                CommittedTransactions.read(
                    server.databaseInternal(),
                    after.appendIndex(),
                    after.checksum(),
                ) {
                    it as Continued
                    assertThat(it.last).isEqualTo(server.lastCommitted())
                    it.transactions.toList()
                }

            assertThat(fields(read)).isEqualTo(fields(expected)).hasSizeGreaterThan(10)
            assertThat(read.map { it.previousChecksum })
                .isEqualTo(listOf(after.checksum()) + read.dropLast(1).map { it.checksum })
        }
    }

    @Test
    fun `reads the transactions after the base one, which no log holds, as after an import`() {
        CommunityServer(root).start().use { server ->
            CommunityServer.populate(server)
            val base = AppendIndexProvider.BASE_APPEND_INDEX
            val database = server.databaseInternal()

            val read =
                CommittedTransactions.read(database, base, TransactionIdStore.BASE_TX_CHECKSUM) {
                    (it as Continued).transactions.toList()
                }
            val diverged =
                CommittedTransactions.read(
                    database,
                    base,
                    TransactionIdStore.BASE_TX_CHECKSUM + 1,
                ) {
                    it
                }

            assertThat(read.map { it.appendIndex })
                .isEqualTo((base + 1..server.lastCommitted().appendIndex()).toList())
            assertThat(read.first().previousChecksum).isEqualTo(TransactionIdStore.BASE_TX_CHECKSUM)
            assertThat(fields(read)).isEqualTo(fields(server.transactionsAfter(base)))
            assertThat(diverged).isInstanceOf(Diverged::class.java)
        }
    }

    @Test
    fun `stops at the last transaction closed when reading began, while others commit`() {
        CommunityServer(root).start().use { server ->
            val after = server.lastCommitted()
            val writing = AtomicBoolean(true)
            val writer = thread {
                while (writing.get()) server.database().executeTransactionally("CREATE (:During)")
            }
            try {
                repeat(20) {
                    val before = server.lastCommitted().appendIndex()
                    while (server.lastCommitted().appendIndex() < before + 3) Thread.sleep(1)
                    CommittedTransactions.read(
                        server.databaseInternal(),
                        after.appendIndex(),
                        after.checksum(),
                    ) {
                        it as Continued
                        val read = it.transactions.toList()
                        assertThat(read.map { t -> t.appendIndex })
                            .isEqualTo((after.appendIndex() + 1..it.last.appendIndex()).toList())
                        assertThat(read.lastOrNull()?.checksum ?: after.checksum())
                            .isEqualTo(it.last.checksum())
                    }
                }
            } finally {
                writing.set(false)
                writer.join()
            }
        }
    }

    @Test
    fun `finds nothing after the database's last transaction`() {
        CommunityServer(root).start().use { server ->
            val last = server.lastCommitted()
            CommittedTransactions.read(
                server.databaseInternal(),
                last.appendIndex(),
                last.checksum(),
            ) {
                it as Continued
                assertThat(it.last).isEqualTo(last)
                assertThat(it.transactions.toList()).isEmpty()
            }
        }
    }

    @Test
    fun `tells when the database does not continue the backup`() {
        CommunityServer(root).start().use { server ->
            server.database().executeTransactionally("CREATE (:One)")
            val middle = server.lastCommitted()
            server.database().executeTransactionally("CREATE (:Two)")
            val last = server.lastCommitted()
            val database = server.databaseInternal()

            fun reason(after: Long, checksum: Int) =
                CommittedTransactions.read(database, after, checksum) {
                    (it as Diverged).reason
                }

            assertThat(reason(middle.appendIndex(), middle.checksum() + 1))
                .isEqualTo(
                    "the database's transaction at append index ${middle.appendIndex()} has " +
                        "checksum ${middle.checksum()}, not the copy's ${middle.checksum() + 1}"
                )
            assertThat(reason(last.appendIndex(), last.checksum() + 1))
                .isEqualTo(
                    "the database's transaction at append index ${last.appendIndex()} has " +
                        "checksum ${last.checksum()}, not the copy's ${last.checksum() + 1}"
                )
            assertThat(server.checksumAt(middle.appendIndex())).isEqualTo(middle.checksum())
        }
    }

    @Test
    fun `tells when the database is behind the copy, which it cannot judge then`() {
        CommunityServer(root).start().use { server ->
            server.database().executeTransactionally("CREATE (:One)")
            val last = server.lastCommitted()

            val behind =
                CommittedTransactions.read(server.databaseInternal(), last.appendIndex() + 1, 0) {
                    it as Behind
                }

            assertThat(behind.last.appendIndex()).isEqualTo(last.appendIndex())
        }
    }

    @Test
    fun `tells when the logs no longer hold the transactions after the backup`() {
        val settings =
            mapOf<Setting<*>, Any>(
                GraphDatabaseSettings.logical_log_rotation_threshold to ByteUnit.kibiBytes(128),
                GraphDatabaseSettings.keep_logical_logs to "false",
            )
        CommunityServer(root, settings = settings).start().use { server ->
            server.database().executeTransactionally("CREATE (:Before)")
            val after = server.lastCommitted()
            CommunityServer.populate(server)
            server.checkPointer().forceCheckPoint(SimpleTriggerInfo("test"))

            val range =
                CommittedTransactions.read(
                    server.databaseInternal(),
                    after.appendIndex(),
                    after.checksum(),
                ) {
                    it
                }

            assertThat(range).isEqualTo(Pruned)
        }
    }

    @Test
    fun `holds off checkpoints while the transactions are read`() {
        CommunityServer(root).start().use { server ->
            val after = server.lastCommitted()
            server.database().executeTransactionally("CREATE (:Later)")
            val checkPointer = server.checkPointer()
            lateinit var checkpoint: Thread
            CommittedTransactions.read(
                server.databaseInternal(),
                after.appendIndex(),
                after.checksum(),
            ) {
                checkpoint = thread {
                    checkPointer.forceCheckPoint(SimpleTriggerInfo("test"))
                }
                checkpoint.join(TimeUnit.SECONDS.toMillis(1))
                assertThat(checkpoint.isAlive).describedAs("checkpoint waiting").isTrue
                it as Continued
                assertThat(it.transactions.count().toLong())
                    .isEqualTo(it.last.appendIndex() - after.appendIndex())
                    .isPositive
            }
            checkpoint.join(TimeUnit.SECONDS.toMillis(30))
            assertThat(checkpoint.isAlive).describedAs("checkpoint waiting").isFalse
        }
    }
}
