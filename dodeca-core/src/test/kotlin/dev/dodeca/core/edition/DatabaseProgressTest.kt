package dev.dodeca.core.edition

import dev.dodeca.core.edition.DatabaseState.Status
import dev.dodeca.core.servers.CopyMode
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/** Where a server stands with a database that WAIT waits for. */
class DatabaseProgressTest {
    private val id = UUID.randomUUID()
    private val online = WantedDatabase("films", id, online = true)
    private val offline = WantedDatabase("films", id, online = false)

    private val server =
        object : HostingServer {
            override val id: UUID = UUID.randomUUID()
            override val address = "server"

            override fun databases() = emptyList<DatabaseState>()
        }

    private fun state(of: DatabaseState, wanted: WantedDatabase): ServerProgress.State? =
        DatabaseProgress.progressOf(listOf(of), wanted)?.invoke(server)?.state

    @Test
    fun `a database wanted online is caught up once it runs and its leader is known`() {
        assertThat(state(DatabaseState("films", id, Status.ONLINE), online))
            .isEqualTo(ServerProgress.State.CaughtUp)
        assertThat(state(DatabaseState("films", id, Status.ONLINE, leaderKnown = false), online))
            .isNull()
        assertThat(state(DatabaseState("films", id, Status.STARTING), online)).isNull()
        assertThat(state(DatabaseState("films", id, Status.FAILED, "full disk"), online))
            .isEqualTo(ServerProgress.State.Failed)
    }

    @Test
    fun `a secondary wanted online is caught up once it runs, as it follows no leader`() {
        val secondary =
            DatabaseState(
                "films",
                id,
                Status.ONLINE,
                leaderKnown = false,
                mode = CopyMode.SECONDARY,
            )

        assertThat(state(secondary, online)).isEqualTo(ServerProgress.State.CaughtUp)
        assertThat(state(secondary.copy(status = Status.COPYING), online)).isNull()
    }

    @Test
    fun `a database wanted offline is caught up once it no longer runs`() {
        assertThat(state(DatabaseState("films", id, Status.STARTING), offline)).isNull()
        assertThat(state(DatabaseState("films", id, Status.ONLINE, leaderKnown = false), offline))
            .isNull()
        assertThat(state(DatabaseState("films", id, Status.OFFLINE, leaderKnown = false), offline))
            .isEqualTo(ServerProgress.State.CaughtUp)
    }
}
