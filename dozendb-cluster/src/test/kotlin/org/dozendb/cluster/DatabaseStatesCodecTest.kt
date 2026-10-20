package org.dozendb.cluster

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.edition.DatabaseState.Status
import org.junit.jupiter.api.Test

class DatabaseStatesCodecTest {
    @Test
    fun `reads back what it writes`() {
        val states =
            listOf(
                DatabaseState("system", UUID(0, 1), Status.ONLINE),
                DatabaseState("film.archive", UUID.randomUUID(), Status.OFFLINE),
                DatabaseState("books", UUID.randomUUID(), Status.FAILED, "Unable to start books"),
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
    fun `refuses what it did not write`() {
        assertThatThrownBy { DatabaseStatesCodec.decode("books") }
            .isInstanceOf(IllegalArgumentException::class.java)
    }
}
