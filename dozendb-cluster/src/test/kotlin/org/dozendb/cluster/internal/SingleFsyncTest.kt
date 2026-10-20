package org.dozendb.cluster.internal

import java.nio.file.Path
import java.util.concurrent.ConcurrentHashMap
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.internal.durability.RecordingFileSystem
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper

/** Commits to replicated databases force only the Raft log. */
class SingleFsyncTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun ClusterHarness.write(cypher: String) {
        awaitLeader().database().executeTransactionally(cypher)
    }

    private fun isTransactionLog(path: Path): Boolean =
        path.fileName.toString().startsWith(TransactionLogFilesHelper.DEFAULT_NAME) &&
            path.parent.fileName.toString() == ClusterHarness.DEFAULT_DATABASE

    @Test
    fun `commits do not force the transaction log`() {
        val systems = ConcurrentHashMap<Int, RecordingFileSystem>()
        val cluster =
            ClusterHarness(
                    root,
                    3,
                    { member -> RecordingFileSystem().also { systems[member.index] = it } },
                )
                .also { harness = it }
        cluster.start()
        cluster.write("CREATE (:Warmup)")
        val before = systems.mapValues { (_, fs) -> fs.forces(::isTransactionLog) }

        repeat(COMMITS) { cluster.write("CREATE (:Node {i: $it})") }

        val forces = systems.mapValues { (index, fs) ->
            fs.forces(::isTransactionLog) - before[index]!!
        }
        // Page cache write-backs force it (the write-ahead rule), commits do not.
        assertThat(forces.values)
            .describedAs("transaction log forces per member during $COMMITS commits")
            .allMatch { it <= COMMITS / 10 }
    }

    private companion object {
        const val COMMITS = 50
    }
}
