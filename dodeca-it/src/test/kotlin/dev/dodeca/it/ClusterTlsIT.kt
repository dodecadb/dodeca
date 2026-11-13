package dev.dodeca.it

import dev.dodeca.consensus.testing.TestAuthority
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.Test
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.lifecycle.Startables

/** How members are configured for TLS between them, and how they take new certificates. */
class ClusterTlsIT {
    @Test
    fun `a member without a choice between TLS and plaintext does not start, and says why`() {
        val refusal = "Clustering needs a choice for the traffic between members"
        DodecaContainer(DodecaContainer.dodecaImage, "core1")
            .withEnv("NEO4J_dbms_dodeca_cluster_enabled", "true")
            .withEnv("NEO4J_dbms_dodeca_cluster_initial__members", "core1:7700")
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

    @Test
    fun `members move to another authority by reloading TLS, for the cluster, Bolt and HTTPS`() {
        val old = TestAuthority("Old CA")
        val new = TestAuthority("New CA")
        val aliases = listOf("reload1", "reload2", "reload3")
        Network.newNetwork().use { network ->
            val members = aliases.map { alias ->
                DodecaContainer(DodecaContainer.dodecaImage, alias)
                    .inNetwork(network)
                    .clusterMember(aliases.map { "$it:7700" }, authority = old)
                    .boltTls(old)
                    .httpsTls(old)
                    .withEnv("NEO4J_dbms_security_tls__reload__enabled", "true")
            }
            try {
                Startables.deepStart(members).join()
                replicates(members)
                fun issuer(member: DodecaContainer, port: Int) =
                    member.presentedCertificate(port).issuerX500Principal.name

                // Each step on every member, reloading, as the operator guide says: trust the new
                // authority, then take certificates from it, then stop trusting the old one.
                for ((issuer, trusted) in
                    listOf(
                        old to listOf(old, new),
                        new to listOf(old, new),
                        new to listOf(new),
                    )) {
                    for (member in members) {
                        for (scope in listOf("cluster", "bolt", "https")) {
                            member.replaceCertificates(
                                scope,
                                issuer.issue(names = listOf(member.alias)),
                                trusted,
                            )
                        }
                        member.rows("CALL dbms.security.reloadTLS()", "system")
                        replicates(members)
                    }
                }

                for (member in members) {
                    assertThat(issuer(member, DodecaContainer.BOLT_PORT)).isEqualTo("CN=New CA")
                    assertThat(issuer(member, DodecaContainer.HTTPS_PORT)).isEqualTo("CN=New CA")
                    assertThat(member.logs).contains("Reloaded the certificate and trusted")
                }
                // A member that connects anew finds the others present certificates of the new
                // authority, which alone it trusts, and accept its own.
                members.last().shutDown()
                members.last().restart()
                replicates(members)
            } finally {
                members.forEach {
                    it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
                    it.stop()
                }
            }
        }
    }

    private fun role(member: DodecaContainer): Any? =
        when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == "neo4j" }?.get("role")
            is Outcome.Failed -> null
        }

    /** Writes on the leader of `neo4j` and waits until every member has the write. */
    private fun replicates(members: List<DodecaContainer>) {
        val leader =
            await.pollDelay(Duration.ZERO) withPollInterval
                Duration.ofMillis(200) atMost
                Duration.ofMinutes(1) untilNotNull
                {
                    members.singleOrNull { role(it) == "LEADER" }
                }
        val title = UUID.randomUUID().toString()
        leader.rows("CREATE (:Film {title: \$title})", parameters = mapOf("title" to title))
        await withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                for (member in members) {
                    assertThat(
                            member
                                .rows(
                                    "MATCH (f:Film {title: \$title}) RETURN count(f) AS films",
                                    parameters = mapOf("title" to title),
                                )
                                .single()["films"]
                        )
                        .describedAs(member.alias)
                        .isEqualTo(1L)
                }
            }
    }
}
