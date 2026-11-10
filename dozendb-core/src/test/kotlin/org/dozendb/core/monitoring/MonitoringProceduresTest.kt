package org.dozendb.core.monitoring

import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.core.StandaloneServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.storageengine.api.TransactionIdStore

/** Neo4j Enterprise's procedures for looking into a running server. */
class MonitoringProceduresTest {
    @TempDir lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    @Test
    fun `db checkpoint checkpoints the database it runs on, up to its last transaction`() {
        server.rows("CREATE (:Film)")
        val database = server.database().dependencyResolver
        val last = database.resolveDependency(TransactionIdStore::class.java)
        val checkPointer = database.resolveDependency(CheckPointer::class.java)
        assertThat(checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId().id())
            .isLessThan(last.lastCommittedTransactionId)

        val rows = server.rows("CALL db.checkpoint()")

        assertThat(rows)
            .containsExactly(mapOf("success" to true, "message" to "Checkpoint completed."))
        assertThat(checkPointer.latestCheckPointInfo().highestObservedClosedTransactionId().id())
            .isEqualTo(last.lastCommittedTransactionId)
    }
}
