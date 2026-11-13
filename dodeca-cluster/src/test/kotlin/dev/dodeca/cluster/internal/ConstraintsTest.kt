package dev.dodeca.cluster.internal

import dev.dodeca.core.ConstraintChecks.assertEnforced
import dev.dodeca.core.ConstraintChecks.assertExistingDataChecked
import dev.dodeca.core.ConstraintChecks.assertTypesEnforced
import java.nio.file.Path
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** Property existence, key and type constraints on replicated databases. */
class ConstraintsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val cluster by lazy { ClusterHarness(root, 1) }

    @AfterEach fun stop() = cluster.close()

    @Test
    fun `existence constraints are enforced and survive a restart`() {
        cluster.start()
        val member = cluster.members.single()

        assertEnforced(
            write = { cluster.awaitLeader().database().executeTransactionally(it) },
            read = { member.database().executeTransactionally(it, emptyMap()) { r -> r.next() } },
            restart = {
                member.stop()
                member.start()
            },
        )
    }

    @Test
    fun `property type constraints are enforced and survive a restart`() {
        cluster.start()
        val member = cluster.members.single()

        assertTypesEnforced(
            write = { cluster.awaitLeader().database().executeTransactionally(it) },
            restart = {
                member.stop()
                member.start()
            },
        )
    }

    @Test
    fun `creating existence and key constraints checks existing data`() {
        cluster.start()

        assertExistingDataChecked { cluster.awaitLeader().database().executeTransactionally(it) }
    }
}
