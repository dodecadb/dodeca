package dev.dodeca.consensus.ratis

import org.apache.ratis.util.VersionInfo
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PatchedRatisClassesTest {
    @Test
    fun `are copies of the classes of the Ratis version in use`() {
        // After a Ratis upgrade, PatchedGrpcLogAppender, PatchedGrpcServicesImpl and
        // PatchedGrpcServerProtocolClient are the new Ratis classes they copy, with the changes
        // marked Dodeca, or go once Ratis has them; see doc/UPSTREAM_TOUCHPOINTS.md.
        assertThat(VersionInfo.getSoftwareInfoVersion()).isEqualTo("3.3.1")
    }
}
