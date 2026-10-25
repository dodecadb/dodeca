package org.dozendb.cluster.internal.durability

import java.nio.ByteBuffer
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardOpenOption
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.StoreChannel

/** What the power loss of [PowerLossFileSystem] loses: only what the log files did not force. */
class PowerLossFileSystemTest {
    @TempDir lateinit var root: Path
    private val fs = PowerLossFileSystem()

    private fun log(directory: Path): Path =
        Files.createDirectories(directory.resolve("neo4j")).resolve("neostore.transaction.db.0")

    private fun StoreChannel.append(bytes: Int) {
        writeAll(ByteBuffer.wrap(ByteArray(bytes) { 7 }))
    }

    /** The bytes of [path] up to [length] that are no longer what was written. */
    private fun lost(path: Path, length: Int): Int =
        Files.readAllBytes(path).take(length).count { it != 7.toByte() }

    @Test
    fun `what a log file held before it was opened here survives`() {
        val log = log(root)
        Files.write(log, ByteArray(4096) { 7 })

        fs.open(log, setOf(StandardOpenOption.WRITE)).use {
            it.position(4096)
            it.append(4096)
        }
        fs.cut()
        fs.loseUnforcedWrites(Random(1))

        assertThat(lost(log, 4096)).isZero()
    }

    @Test
    fun `a forced log file moved into place keeps what it forced`() {
        val staged = log(root.resolve("staging"))
        fs.write(staged).use {
            it.append(4096)
            it.force(false)
        }
        val target = root.resolve("transactions")
        fs.renameFile(staged.parent.parent, target)
        val log = target.resolve("neo4j").resolve(staged.fileName)

        fs.open(log, setOf(StandardOpenOption.WRITE)).use {
            it.position(4096)
            it.append(4096)
        }
        fs.cut()
        val zeroed = fs.loseUnforcedWrites(Random(1))

        assertThat(lost(log, 4096)).isZero()
        assertThat(zeroed).isPositive()
    }
}
