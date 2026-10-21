package org.dozendb.backup

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.EOFException
import java.io.IOException
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.configuration.Config
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.fs.OutputStreamWritableChannel
import org.neo4j.io.fs.PhysicalFlushableChannel
import org.neo4j.kernel.BinarySupportedKernelVersions
import org.neo4j.kernel.impl.transaction.log.LogIndexEncoding
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryWriter
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.CommandReaderFactory

class DifferentialBackupTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: CommunityServer
    private lateinit var readers: CommandReaderFactory
    private val fs = DefaultFileSystemAbstraction()

    @BeforeEach
    fun start() {
        server = CommunityServer(root).start()
        readers = server.commandReaders()
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun write(header: ArtifactHeader, transactions: List<ArtifactTransaction>): ByteArray {
        val out = ByteArrayOutputStream()
        val staging = root.resolve("staging")
        Artifacts.writeDifferential(out, header, transactions.asSequence(), staging, fs)
        assertThat(staging).doesNotExist()
        return out.toByteArray()
    }

    private fun header(transactions: List<ArtifactTransaction>, compressed: Boolean = true) =
        server.differentialHeader(transactions, compressed)

    private fun read(artifact: ByteArray): Pair<ArtifactHeader, List<ArtifactTransaction>> =
        Artifacts.readDifferential(ByteArrayInputStream(artifact), readers) { header, transactions
            ->
            header to transactions.toList()
        }

    /** Everything Neo4j writes of [transaction] to its log, comparable. */
    private fun contents(transaction: ArtifactTransaction): List<Any> {
        val batch = transaction.batch
        val commands = ByteArrayOutputStream()
        val channel = OutputStreamWritableChannel(commands)
        batch.accept { command ->
            command.serialize(channel)
            false
        }
        return listOf(
            transaction.transactionId,
            transaction.appendIndex,
            transaction.previousChecksum,
            transaction.checksum,
            batch.kernelVersion(),
            batch.timeStarted,
            batch.latestCommittedTxWhenStarted,
            batch.timeCommitted,
            batch.consensusIndex(),
            batch.commandCount(),
            commands.toByteArray().toList(),
        )
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `reads back every transaction as Neo4j's log holds it`(compressed: Boolean) {
        val after = server.lastCommitted().appendIndex()
        CommunityServer.populate(server)
        val transactions = server.transactionsAfter(after)
        assertThat(transactions).hasSizeGreaterThan(10)
        val header = header(transactions, compressed)

        val (read, readTransactions) = read(write(header, transactions))

        assertThat(read).isEqualTo(header)
        assertThat(readTransactions.map(::contents)).isEqualTo(transactions.map(::contents))
        assertThat(readTransactions.last().checksum).isEqualTo(server.lastCommitted().checksum())
    }

    @Test
    fun `Neo4j appends the transactions read back with the checksums its log recorded`() {
        val after = server.lastCommitted().appendIndex()
        CommunityServer.populate(server)
        val transactions = server.transactionsAfter(after)
        val (_, readTransactions) = read(write(header(transactions), transactions))

        // What restore does, through the writer Neo4j's TransactionLogWriter appends with.
        val checksums =
            PhysicalFlushableChannel(
                    fs.write(root.resolve("appended")),
                    EmptyMemoryTracker.INSTANCE,
                )
                .use { channel ->
                    val writer =
                        LogEntryWriter(channel, BinarySupportedKernelVersions(Config.defaults()))
                    readTransactions.map { transaction ->
                        val batch = transaction.batch
                        val version = batch.kernelVersion()
                        writer.writeStartEntry(
                            version,
                            batch.timeStarted,
                            batch.latestCommittedTxWhenStarted,
                            transaction.appendIndex,
                            transaction.previousChecksum,
                            LogIndexEncoding.encodeLogIndex(batch.consensusIndex()),
                        )
                        writer.serialize(batch, version)
                        writer.writeCommitEntry(
                            version,
                            transaction.transactionId,
                            batch.timeCommitted,
                        )
                    }
                }

        assertThat(checksums).isEqualTo(transactions.map { it.checksum })
    }

    @Test
    fun `keeps the Raft index of a replicated transaction`() {
        val after = server.lastCommitted().appendIndex()
        server.database().executeTransactionally("CREATE (:Replicated)")
        // The last one: the label's token commits first, on its own.
        val replicated = server.transactionsAfter(after).last()
        // Community commits without a Raft index; DozenDB's clustered members commit with one.
        replicated.batch.setConsensusIndex(4242)

        val (_, transactions) = read(write(header(listOf(replicated)), listOf(replicated)))

        assertThat(transactions.single().batch.consensusIndex()).isEqualTo(4242)
    }

    @Test
    fun `an empty differential backup holds no transactions`() {
        val header = header(emptyList())

        val (read, transactions) = read(write(header, emptyList()))

        assertThat(read.empty).isTrue
        assertThat(transactions).isEmpty()
    }

    @Test
    fun `refuses to write transactions other than the header tells`() {
        val after = server.lastCommitted().appendIndex()
        CommunityServer.populate(server)
        val transactions = server.transactionsAfter(after)
        val header = header(transactions)

        assertThatThrownBy { write(header, transactions.drop(1)) }
            .isInstanceOf(IOException::class.java)
            .hasMessageContaining("but its header tells append index ${header.lowestAppendIndex}")
        assertThatThrownBy { write(header, transactions.filterIndexed { index, _ -> index != 3 }) }
            .hasMessage("${transactions[4]} does not follow ${transactions[2]}")
        assertThatThrownBy { write(header, transactions.dropLast(1)) }
            .hasMessage(
                "the backup ends with ${transactions[transactions.size - 2]}, but its header " +
                    "tells append index ${header.highestAppendIndex}"
            )
        assertThatThrownBy {
                write(header.copy(lastChecksum = header.lastChecksum!! + 1), transactions)
            }
            .hasMessageContaining("${transactions.last()} has checksum")
        assertThatThrownBy { write(header(emptyList()), transactions) }
            .hasMessage("an empty backup holds ${transactions.first()}")
    }

    @Test
    fun `a corrupt or truncated backup fails to read`() {
        val after = server.lastCommitted().appendIndex()
        CommunityServer.populate(server)
        val transactions = server.transactionsAfter(after)
        val artifact = write(header(transactions, compressed = false), transactions)

        val corrupt = artifact.copyOf().also { it[it.size / 2] = (it[it.size / 2] + 1).toByte() }
        assertThatThrownBy { read(corrupt) }
            .isInstanceOf(IOException::class.java)
            .hasMessageEndingWith("is corrupt")
        for (length in listOf(artifact.size - 1, artifact.size - 3, artifact.size / 2)) {
            assertThatThrownBy { read(artifact.copyOf(length)) }
                .isInstanceOf(EOFException::class.java)
                .hasMessage("the backup ends before its last transaction")
        }
        assertThatThrownBy { read(artifact + 0) }
            .hasMessage("unexpected bytes after the last transaction")
    }
}
