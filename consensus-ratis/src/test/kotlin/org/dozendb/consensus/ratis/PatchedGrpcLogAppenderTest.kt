package org.dozendb.consensus.ratis

import org.apache.ratis.util.VersionInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PatchedGrpcLogAppenderTest {
    @Test
    fun `is a copy of the log appender of the Ratis version in use`() {
        // After a Ratis upgrade, PatchedGrpcLogAppender is the new GrpcLogAppender with the changes
        // marked DozenDB, or goes once Ratis ends the streams it gives up on.
        assertThat(VersionInfo.getSoftwareInfoVersion()).isEqualTo("3.3.1")
    }
}
