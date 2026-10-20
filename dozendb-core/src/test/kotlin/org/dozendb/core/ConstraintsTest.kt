package org.dozendb.core

import java.nio.file.Path
import org.dozendb.core.ConstraintChecks.assertEnforced
import org.dozendb.core.ConstraintChecks.assertExistingDataChecked
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir

/** Property existence and key constraints without clustering. */
class ConstraintsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private val server by lazy { StandaloneServer(root) }

    @AfterEach fun stop() = server.close()

    @Test
    fun `existence constraints are enforced and survive a restart`() {
        server.start()

        assertEnforced(
            write = { server.database().executeTransactionally(it) },
            read = { server.rows(it).single() },
            restart = server::restart,
        )
    }

    @Test
    fun `creating existence and key constraints checks existing data`() {
        server.start()

        assertExistingDataChecked { server.database().executeTransactionally(it) }
    }
}
