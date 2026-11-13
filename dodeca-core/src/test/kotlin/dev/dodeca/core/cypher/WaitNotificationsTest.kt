package dev.dodeca.core.cypher

import dev.dodeca.core.edition.ServerProgress
import dev.dodeca.core.edition.ServerProgress.State
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.cypher.internal.notification.WaitServerCatchingUp
import org.neo4j.cypher.internal.notification.WaitServerCaughtUp
import org.neo4j.cypher.internal.notification.WaitServerFailed
import org.neo4j.cypher.internal.notification.WaitServerUnavailable

/** The notifications that WAIT gives in Cypher 25, as Neo4j Enterprise's, one per server. */
class WaitNotificationsTest {
    @Test
    fun `each server's progress is one of Enterprise's notifications, naming the server`() {
        val named = UUID.randomUUID()
        val unnamed = UUID.randomUUID()

        val notifications =
            WaitExecutionPlan.notifications(
                listOf(
                    ServerProgress(named, "east:7687", State.CaughtUp, "caught up"),
                    ServerProgress(named, "east:7687", State.Failed, "full disk"),
                    ServerProgress(unnamed, "west:7687", State.TimedOut, "not caught up"),
                    ServerProgress(
                        unnamed,
                        "west:7687",
                        State.TimedOut,
                        "could not be asked: refused",
                        available = false,
                    ),
                ),
                mapOf(named to "east"),
            )

        assertThat(notifications)
            .containsExactly(
                WaitServerCaughtUp("east", "east:7687"),
                WaitServerFailed("east", "east:7687", "full disk"),
                // A server without a record is named by its id, as SHOW SERVERS names it.
                WaitServerCatchingUp("$unnamed", "west:7687"),
                WaitServerUnavailable("$unnamed"),
            )
    }
}
