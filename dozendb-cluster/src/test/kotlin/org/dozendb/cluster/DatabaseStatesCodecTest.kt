package org.dozendb.cluster

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.edition.DatabaseState.Status
import org.dozendb.core.servers.CopyMode
import org.junit.jupiter.api.Test

class DatabaseStatesCodecTest {
    @Test
    fun `reads back what it writes`() {
        val states =
            listOf(
                DatabaseState("system", UUID(0, 1), Status.ONLINE),
                DatabaseState("series", UUID.randomUUID(), Status.ONLINE, leaderKnown = false),
                DatabaseState("music", UUID.randomUUID(), Status.STARTING, leaderKnown = false),
                DatabaseState("film.archive", UUID.randomUUID(), Status.OFFLINE),
                DatabaseState("books", UUID.randomUUID(), Status.FAILED, "Unable to start books"),
                DatabaseState(
                    "maps",
                    UUID.randomUUID(),
                    Status.QUARANTINED,
                    "By neo4j at 2026-11-10T09:08:07.006Z: Inspecting its files",
                ),
                DatabaseState(
                    "poems",
                    UUID.randomUUID(),
                    Status.ONLINE,
                    leaderKnown = false,
                    mode = CopyMode.SECONDARY,
                ),
                DatabaseState(
                    "letters",
                    UUID.randomUUID(),
                    Status.ONLINE,
                    leaderKnown = false,
                    mode = CopyMode.SECONDARY,
                    paused = true,
                ),
            )

        assertThat(DatabaseStatesCodec.decode(DatabaseStatesCodec.encode(states))).isEqualTo(states)
        assertThat(DatabaseStatesCodec.decode(DatabaseStatesCodec.encode(emptyList()))).isEmpty()
    }

    @Test
    fun `keeps a failure on one line`() {
        val failed =
            DatabaseState("books", UUID.randomUUID(), Status.FAILED, "first\nsecond\tthird")

        assertThat(DatabaseStatesCodec.decode(DatabaseStatesCodec.encode(listOf(failed))))
            .containsExactly(failed.copy(failure = "first second third"))
    }

    @Test
    fun `leaves out a database whose status a later version added, and reads the others`() {
        val online = DatabaseState("films", UUID.randomUUID(), Status.ONLINE)
        val later = "books\t${UUID.randomUUID()}\tRESTORING\ttrue\t"

        assertThat(
                DatabaseStatesCodec.decode(
                    later + "\n" + DatabaseStatesCodec.encode(listOf(online))
                )
            )
            .containsExactly(online)
    }

    @Test
    fun `reads a database with fields a later version added`() {
        val online = DatabaseState("films", UUID.randomUUID(), Status.ONLINE)

        assertThat(
                DatabaseStatesCodec.decode(DatabaseStatesCodec.encode(listOf(online)) + "\tlater")
            )
            .containsExactly(online)
    }

    @Test
    fun `reads a database that an earlier version reported as a primary`() {
        val id = UUID.randomUUID()

        assertThat(DatabaseStatesCodec.decode("films\t$id\tONLINE\ttrue\t"))
            .containsExactly(DatabaseState("films", id, Status.ONLINE, mode = CopyMode.PRIMARY))
        assertThat(DatabaseStatesCodec.decode("films\t$id\tONLINE\tfalse\t\tsecondary"))
            .describedAs("before pauses")
            .containsExactly(
                DatabaseState(
                    "films",
                    id,
                    Status.ONLINE,
                    leaderKnown = false,
                    mode = CopyMode.SECONDARY,
                )
            )
    }

    @Test
    fun `leaves out a database whose mode a later version added`() {
        val online = DatabaseState("films", UUID.randomUUID(), Status.ONLINE)
        val later = "books\t${UUID.randomUUID()}\tONLINE\ttrue\t\tarchive"

        assertThat(
                DatabaseStatesCodec.decode(
                    later + "\n" + DatabaseStatesCodec.encode(listOf(online))
                )
            )
            .containsExactly(online)
    }

    @Test
    fun `refuses what it did not write`() {
        assertThatThrownBy { DatabaseStatesCodec.decode("books") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
