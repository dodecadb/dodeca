package org.dozendb.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.testcontainers.containers.wait.strategy.Wait

/** How members are configured for TLS between them. */
class ClusterTlsIT {
    @Test
    fun `a member without a choice between TLS and plaintext does not start, and says why`() {
        val refusal = "Clustering needs a choice for the traffic between members"
        DozenDbContainer(DozenDbContainer.dozenDbImage, "core1")
            .withEnv("NEO4J_dbms_dozendb_cluster_enabled", "true")
            .withEnv("NEO4J_dbms_dozendb_cluster_initial__members", "core1:7700")
            .waitingFor(
                Wait.forLogMessage(".*$refusal.*", 1).withStartupTimeout(Duration.ofMinutes(3))
            )
            .use { member ->
                member.start()

                assertThat(member.logs)
                    .contains("dbms.ssl.policy.cluster.enabled=true")
                    .contains("dbms.ssl.policy.cluster.enabled=false")
                    .doesNotContain("Started.")
            }
    }
}
