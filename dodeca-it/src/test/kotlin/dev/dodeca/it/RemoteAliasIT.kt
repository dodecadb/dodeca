package dev.dodeca.it

import dev.dodeca.consensus.testing.TestAuthority
import java.time.Duration
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.containers.Network
import org.testcontainers.images.builder.Transferable
import org.testcontainers.lifecycle.Startables

/**
 * Remote aliases between two DBMSs: `here` runs their transactions on `there` over TLS, trusting
 * there's certificate through its JVM's trust store, with their passwords encrypted by a key from a
 * keystore, as the operator guide has it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class RemoteAliasIT {
    private val network = Network.newNetwork()
    private val authority = TestAuthority()
    private val there =
        DodecaContainer(DodecaContainer.dodecaImage, "there")
            .inNetwork(network)
            // Reachable by a name its certificate does not have, too.
            .withNetworkAliases(OTHER_NAME)
            .boltTls(authority)
            .aliasKeystore(key())
    private val here =
        DodecaContainer(DodecaContainer.dodecaImage, "here")
            .inNetwork(network)
            .trusting(authority)
            .aliasKeystore(key())

    @BeforeAll
    fun start() {
        Startables.deepStart(there, here).join()
        there.rows("CREATE DATABASE films WAIT", "system")
        there.rows("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED", "system")
        there.rows("GRANT ROLE publisher TO alice", "system")
    }

    @AfterAll
    fun stop() {
        listOf(here, there).forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun key(): SecretKey = KeyGenerator.getInstance("AES").apply { init(256) }.generateKey()

    /** Creates the alias [name] here of [target] there at [url], as alice. */
    private fun alias(
        name: String,
        url: String,
        target: String = "films",
        password: String = "alice-secret-1",
    ) {
        here.rows(
            "CREATE ALIAS `$name` FOR DATABASE `$target` AT '$url' USER alice PASSWORD '$password'",
            "system",
        )
    }

    private fun failure(cypher: String, database: String): Outcome.Failed =
        here.run(cypher, database) as? Outcome.Failed
            ?: throw AssertionError("$cypher in $database succeeded")

    @Test
    @Order(1)
    fun `runs transactions on the other DBMS over TLS that the JVM trusts`() {
        alias("films", "neo4j+s://there:7687")
        alias("films-direct", "bolt+s://there:7687")
        // Any certificate, whatever the name.
        alias("films-any", "neo4j+ssc://$OTHER_NAME:7687")

        here.rows("CREATE (:Film {title: 'Heat'})", "films")
        here.rows("CREATE (:Film {title: 'Ronin'})", "films-direct")

        assertThat(there.rows("MATCH (f:Film) RETURN f.title AS title ORDER BY title", "films"))
            .containsExactly(mapOf("title" to "Heat"), mapOf("title" to "Ronin"))
        for (alias in listOf("films", "films-direct", "films-any")) {
            assertThat(here.rows("MATCH (f:Film) RETURN count(f) AS films", alias))
                .describedAs(alias)
                .containsExactly(mapOf("films" to 2L))
        }
    }

    @Test
    @Order(2)
    fun `refuses a certificate for another name`() {
        alias("elsewhere", "bolt+s://$OTHER_NAME:7687")

        val refused = failure("RETURN 1", "elsewhere")

        assertThat(refused.code).isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(refused.message)
            .startsWith("Could not reach the DBMS of remote alias 'elsewhere'")
    }

    @Test
    @Order(3)
    fun `logs in with the credentials the alias has now`() {
        there.rows("ALTER USER alice SET PASSWORD 'alice-secret-2' CHANGE NOT REQUIRED", "system")
        alias("stale", "neo4j+s://there:7687")

        assertThat(failure("RETURN 1", "stale").message)
            .contains("failed with Neo.ClientError.Security.Unauthorized")

        here.rows("ALTER ALIAS stale SET DATABASE PASSWORD 'alice-secret-2'", "system")
        assertThat(here.rows("MATCH (f:Film) RETURN count(f) AS films", "stale"))
            .containsExactly(mapOf("films" to 2L))
        // So that the alias keeps working once there has dropped its connections.
        here.rows("ALTER ALIAS films SET DATABASE PASSWORD 'alice-secret-2'", "system")
        assertThat(here.rows("MATCH (f:Film) RETURN count(f) AS films", "films"))
            .containsExactly(mapOf("films" to 2L))
    }

    @Test
    @Order(4)
    fun `cannot read passwords encrypted with another key until they are set again`() {
        val path = "/var/lib/neo4j/certificates/aliases-2.p12"
        here.copyFileToContainer(
            Transferable.of(DodecaContainer.keystore(key()), DodecaContainer.READABLE),
            path,
        )
        here.rows("CALL dbms.setConfigValue('dbms.security.keystore.path', '$path')", "system")
        // A new version of the alias decrypts its password anew.
        here.rows(
            "ALTER ALIAS stale SET DATABASE DRIVER {connection_pool_max_size: 10}",
            "system",
        )

        assertThat(failure("RETURN 1", "stale").message)
            .isEqualTo("Failed to decrypt remote user password.")

        here.rows("ALTER ALIAS stale SET DATABASE PASSWORD 'alice-secret-2'", "system")
        assertThat(here.rows("MATCH (f:Film) RETURN count(f) AS films", "stale"))
            .containsExactly(mapOf("films" to 2L))
    }

    @Test
    @Order(5)
    fun `refuses alias chains`() {
        // The other DBMS has a remote alias of its own, of this DBMS.
        there.rows(
            "CREATE ALIAS onwards FOR DATABASE neo4j AT 'bolt://here:7687' USER neo4j " +
                "PASSWORD '${DodecaContainer.PASSWORD}' DRIVER {ssl_enforced: false}",
            "system",
        )
        alias("chain", "neo4j+s://there:7687", target = "onwards", password = "alice-secret-2")

        val chain = failure("RETURN 1", "chain")

        assertThat(chain.code).isEqualTo("Neo.ClientError.Statement.RemoteExecutionClientError")
        assertThat(chain.message)
            .contains("failed with Neo.ClientError.Database.IllegalAliasChain")
            .contains("alias chains are not permitted")
    }

    @Test
    @Order(6)
    fun `reports a DBMS it cannot reach, and runs there again once it is back`() {
        there.shutDown()

        assertThat(failure("RETURN 1", "films").code)
            .isEqualTo("Neo.TransientError.Statement.RemoteExecutionTransientError")

        there.restart()
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                assertThat(here.run("MATCH (f:Film) RETURN count(f) AS films", "films"))
                    .isEqualTo(Outcome.Rows(listOf(mapOf("films" to 2L))))
            }
    }

    private companion object {
        const val OTHER_NAME = "there-by-another-name"
    }
}
