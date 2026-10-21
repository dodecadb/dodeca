package org.dozendb.cluster.internal.storecopy

import java.io.IOException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.consensus.LogPosition
import org.dozendb.consensus.Snapshot
import org.junit.jupiter.api.Test

class CopySnapshotTest {
    @Test
    fun `round trips a snapshot, and its absence`() {
        val snapshot = Snapshot(LogPosition(42, 3), 40)

        assertThat(CopySnapshot.decode(CopySnapshot.encode(snapshot))).isEqualTo(snapshot)
        assertThat(CopySnapshot.decode(CopySnapshot.encode(null))).isNull()
    }

    @Test
    fun `refuses what is not a snapshot`() {
        assertThatThrownBy { CopySnapshot.decode(byteArrayOf(1, 2, 3)) }
            .isInstanceOf(IOException::class.java)
            .hasMessage("invalid snapshot of 3 bytes")
        assertThatThrownBy {
                CopySnapshot.decode(
                    CopySnapshot.encode(Snapshot(LogPosition(42, 3), 40)).also { it[23] = 99 }
                )
            }
            .hasMessageContaining("with applied index")
    }
}
