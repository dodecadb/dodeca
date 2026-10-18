package org.dozendb.cluster.core.internal.durability

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.StoreChannel

class SingleFsyncFileSystemTest {
    @TempDir lateinit var root: Path
    private val recording = RecordingFileSystem()
    private lateinit var fs: SingleFsyncFileSystem
    private var singleFsync = true

    private val databases: Path
        get() = root.resolve("databases")

    private val transactions: Path
        get() = root.resolve("transactions")

    @BeforeEach
    fun setUp() {
        listOf("neo4j", "other").forEach {
            Files.createDirectories(databases.resolve(it))
            Files.createDirectories(transactions.resolve(it))
        }
        fs = SingleFsyncFileSystem(recording, databases, transactions)
        fs.policy = FsyncPolicy { database -> singleFsync && database == "neo4j" }
    }

    private fun log(database: String = "neo4j", version: Int = 0): Path =
        transactions.resolve(database).resolve("neostore.transaction.db.$version")

    private fun checkpoint(database: String = "neo4j"): Path =
        transactions.resolve(database).resolve("checkpoint.0")

    private fun store(database: String = "neo4j"): Path =
        databases.resolve(database).resolve("neostore.nodestore.db")

    private fun StoreChannel.append(text: String = "x") {
        writeAll(ByteBuffer.wrap(text.toByteArray()))
    }

    private fun forcesOf(path: Path): Int = recording.forces { it == path }

    @Test
    fun `commits to a database with single fsync force its transaction log only once`() {
        fs.write(log()).use { channel ->
            channel.append("header")
            channel.flush()
            repeat(3) {
                channel.append()
                channel.flush()
            }
            assertThat(forcesOf(log())).describedAs("the first flush only").isEqualTo(1)
        }
    }

    @Test
    fun `commits force the transaction log while the database does not use single fsync`() {
        singleFsync = false
        fs.write(log()).use { channel ->
            repeat(3) {
                channel.append()
                channel.flush()
            }
            assertThat(forcesOf(log())).isEqualTo(3)
        }
        fs.write(log("other")).use { channel ->
            repeat(2) {
                channel.append()
                channel.flush()
            }
            assertThat(forcesOf(log("other"))).isEqualTo(2)
        }
    }

    @Test
    fun `closing a transaction log forces it once, however often it is closed`() {
        val channel = fs.write(log())
        channel.append()
        channel.close()
        channel.close()
        assertThat(forcesOf(log())).isEqualTo(1)
        assertThat(channel.isOpen).isFalse
    }

    @Test
    fun `a store write forces the database's unforced transaction logs first`() {
        fs.write(log()).use { log ->
            log.append("header")
            log.flush()
            log.append("transaction")
            log.flush()
            fs.write(store()).use { store ->
                recording.events.clear()
                store.writeAll(ByteBuffer.wrap(ByteArray(8)), 0)
                assertThat(recording.events)
                    .containsExactly(
                        "force neostore.transaction.db.0",
                        "write neostore.nodestore.db",
                    )

                recording.events.clear()
                store.writeAll(ByteBuffer.wrap(ByteArray(8)), 8)
                assertThat(recording.events)
                    .describedAs("nothing left to force")
                    .containsExactly("write neostore.nodestore.db")
            }
        }
    }

    @Test
    fun `a checkpoint is written and forced only after the transaction logs`() {
        fs.write(log()).use { log ->
            log.append("header")
            log.flush()
            fs.write(checkpoint()).use { checkpoint ->
                log.append("transaction")
                log.flush()
                recording.events.clear()
                checkpoint.append("checkpoint")
                assertThat(recording.events)
                    .containsExactly("force neostore.transaction.db.0", "write checkpoint.0")

                log.append("transaction")
                log.flush()
                recording.events.clear()
                checkpoint.flush()
                assertThat(recording.events)
                    .containsExactly("force neostore.transaction.db.0", "force checkpoint.0")
            }
        }
    }

    @Test
    fun `writes to other databases and other files force nothing`() {
        fs.write(log()).use { log ->
            log.append("header")
            log.flush()
            log.append("transaction")
            log.flush()
            recording.events.clear()
            fs.write(store("other")).use { it.append() }
            fs.write(root.resolve("unrelated")).use { it.append() }
            fs.write(databases.resolve("neo4j-lookalike")).use { it.append() }
            assertThat(recording.events)
                .containsExactly(
                    "write neostore.nodestore.db",
                    "write unrelated",
                    "write neo4j-lookalike",
                )
        }
    }

    @Test
    fun `channels opened for reading are not wrapped`() {
        Files.writeString(log(), "transaction")
        fs.read(log()).use { channel ->
            channel.force(false)
            channel.force(false)
        }
        assertThat(forcesOf(log())).isEqualTo(2)
    }
}
