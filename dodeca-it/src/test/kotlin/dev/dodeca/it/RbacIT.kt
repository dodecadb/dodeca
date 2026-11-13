package dev.dodeca.it

import dev.dodeca.consensus.testing.TestPorts
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
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
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Roles and privileges in a cluster, through `neo4j://` drivers as applications use them: reads on
 * readers and the writer, denials, impersonation, the roles of auth rules and property rules with
 * user tags, and an admin locked out who gets the role back with auth disabled on every member, as
 * Neo4j's operations manual describes.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class RbacIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val aliases = listOf("core1", "core2", "core3")
    private val volumes = aliases.map { "dodeca-it-$run-$it" }
    // Fixed host ports, so that members started again with other settings keep their addresses.
    private val boltPorts = aliases.map { TestPorts.free() }
    private var members = members()
    private var restarts = 0
    private val drivers = mutableListOf<Driver>()

    /**
     * The members, on their data volumes, with auth rules and user tags on, and the environment
     * [settings] on top of the image's.
     */
    private fun members(settings: Map<String, String> = emptyMap()): List<DodecaContainer> =
        aliases.mapIndexed { index, alias ->
            DodecaContainer(DodecaContainer.dodecaImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .boltOnHostPort(boltPorts[index])
                .withDataVolume(volumes[index])
                .withEnv("NEO4J_dbms_security_abac_authorization__providers", "native")
                .also { member -> settings.forEach(member::withEnv) }
        }

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        val admin = driver(AuthTokens.basic("neo4j", DodecaContainer.PASSWORD))
        for (command in
            listOf(
                "CREATE DATABASE films WAIT",
                "CREATE DATABASE books WAIT",
                "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED",
                "CREATE USER bob SET PASSWORD 'bob-secret-1' CHANGE NOT REQUIRED " +
                    "SET HOME DATABASE films",
                "CREATE ROLE viewer",
                "GRANT ACCESS ON DATABASE films TO viewer",
                "GRANT MATCH {*} ON GRAPH films NODES Movie TO viewer",
                "GRANT ROLE viewer TO alice",
                "CREATE ROLE critic AS COPY OF viewer",
                "GRANT ROLE critic TO bob",
            )) {
            admin.system(command)
        }
        admin.session(SessionConfig.forDatabase("films")).use { session ->
            session.executeWrite { it.run("CREATE (:Movie {title: 'Alien'}), (:Book)").consume() }
        }
    }

    @AfterAll
    fun stop() {
        drivers.forEach(Driver::close)
        members.forEach {
            it.saveLogs(logs())
            it.stop()
        }
        volumes.forEach(DodecaContainer::removeVolume)
        network.close()
    }

    /** Where the logs of the members started last go. */
    private fun logs() =
        DodecaContainer.logsDirectory.resolve(javaClass.simpleName).resolve("start-$restarts")

    /**
     * A routing driver seeded with every member, as a DNS name with a record per member does, so
     * that it finds the cluster whichever members run.
     */
    private fun driver(auth: AuthToken): Driver =
        GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.first()}",
                auth,
                Config.builder()
                    .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
            .also { drivers += it }

    private fun Driver.system(command: String) {
        session(SessionConfig.forDatabase("system")).use { it.run(command).consume() }
    }

    /** The titles of the nodes [config]'s session finds, by a reader and then by the writer. */
    private fun Driver.titles(config: SessionConfig): List<List<String?>> =
        session(config).use { session ->
            val query = "MATCH (n) RETURN n.title AS title ORDER BY title"
            listOf(
                session.executeRead { tx -> tx.run(query).list { it["title"].asString(null) } },
                session.executeWrite { tx -> tx.run(query).list { it["title"].asString(null) } },
            )
        }

    private fun eventually(assertion: () -> Unit) {
        await.ignoreExceptions() withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                assertion()
            }
    }

    @Test
    @Order(1)
    fun `users find what their roles let them on readers and the writer, and nowhere else`() {
        val alice = driver(AuthTokens.basic("alice", "alice-secret-1"))

        // Followers apply the roles a moment after the leader.
        eventually {
            assertThat(alice.titles(SessionConfig.forDatabase("films")))
                .containsExactly(listOf("Alien"), listOf("Alien"))
        }
        assertThat(
                catchThrowable {
                    alice.session(SessionConfig.forDatabase("films")).use { session ->
                        session.executeWrite { it.run("CREATE (:Movie)").consume() }
                    }
                }
            )
            .isInstanceOf(ClientException::class.java)
            .hasMessage(
                "Create node with labels 'Movie' on database 'films' is not allowed for user " +
                    "'alice' with roles [PUBLIC, viewer]."
            )
        assertThat(catchThrowable { alice.titles(SessionConfig.forDatabase("books")) })
            .isInstanceOf(ClientException::class.java)
            .hasMessage(
                "Database access is not allowed for user 'alice' with roles [PUBLIC, viewer]."
            )
    }

    @Test
    @Order(2)
    fun `an impersonated user's privileges and home database apply through routing`() {
        val admin = driver(AuthTokens.basic("neo4j", DodecaContainer.PASSWORD))
        val alice = driver(AuthTokens.basic("alice", "alice-secret-1"))
        val asBob = SessionConfig.builder().withImpersonatedUser("bob").build()

        assertThat(catchThrowable { alice.titles(asBob) })
            .isInstanceOf(ClientException::class.java)
            .hasMessageStartingWith("Permission has not been granted for IMPERSONATE.")

        admin.system("GRANT IMPERSONATE (bob) ON DBMS TO viewer")
        admin.system("GRANT MATCH {*} ON GRAPH films NODES Book TO critic")

        // The driver asks for bob's home database, films, and bob sees the books too.
        eventually {
            assertThat(alice.titles(asBob))
                .containsExactly(listOf("Alien", null), listOf("Alien", null))
        }
        alice.session(asBob).use { session ->
            assertThat(session.run("CALL db.info() YIELD name").single()["name"].asString())
                .isEqualTo("films")
        }
    }

    @Test
    @Order(3)
    fun `auth rules give roles by user tags, whose property rules apply on readers and the writer`() {
        val admin = driver(AuthTokens.basic("neo4j", DodecaContainer.PASSWORD))
        admin.session(SessionConfig.forDatabase("films")).use { session ->
            session.executeWrite {
                it.run(
                        "CREATE (:Movie {title: 'Solaris', genre: 'scifi'}), " +
                            "(:Movie {title: 'Heat', genre: 'crime'})"
                    )
                    .consume()
            }
        }
        for (command in
            listOf(
                "CREATE USER dana SET PASSWORD 'dana-secret-1' CHANGE NOT REQUIRED " +
                    "SET TAGS ['scifi']",
                "CREATE ROLE genre_reader",
                "GRANT ACCESS ON DATABASE films TO genre_reader",
                "GRANT MATCH {*} ON GRAPH films FOR (n:Movie) WHERE n.genre IN " +
                    "abac.native.user_tags() TO genre_reader",
                "CREATE AUTH RULE tagged SET CONDITION size(abac.native.user_tags()) > 0",
                "GRANT ROLE genre_reader TO AUTH RULE tagged",
            )) {
            admin.system("CYPHER 25 $command")
        }
        val dana = driver(AuthTokens.basic("dana", "dana-secret-1"))
        val films = SessionConfig.forDatabase("films")

        // Followers apply the rules a moment after the leader.
        eventually {
            assertThat(dana.titles(films)).containsExactly(listOf("Solaris"), listOf("Solaris"))
        }
        admin.system("CYPHER 25 ALTER USER dana SET TAGS ['crime', 'scifi']")
        eventually {
            assertThat(dana.titles(films))
                .containsExactly(listOf("Heat", "Solaris"), listOf("Heat", "Solaris"))
        }
        // Without tags the rule does not hold, so dana has no access to films at all.
        admin.system("CYPHER 25 ALTER USER dana SET TAGS []")
        eventually {
            assertThat(catchThrowable { dana.titles(films) })
                .isInstanceOf(ClientException::class.java)
                .hasMessage("Database access is not allowed for user 'dana' with roles [PUBLIC].")
        }
    }

    @Test
    @Order(4)
    fun `an admin locked out gets the role back with auth disabled on every member`() {
        val admin = driver(AuthTokens.basic("neo4j", DodecaContainer.PASSWORD))
        admin.system("REVOKE ROLE admin FROM neo4j")
        eventually {
            assertThat(
                    catchThrowable {
                        admin.system("CREATE USER carol SET PASSWORD 'carol-secret-1'")
                    }
                )
                .isInstanceOf(ClientException::class.java)
                .hasMessageStartingWith("Permission has not been granted for CREATE USER.")
        }

        // As the operator guide has it: on the member that leads system, over bolt://.
        restart(mapOf("NEO4J_dbms_security_auth__enabled" to "false"))
        eventually {
            val leader = members.single { member ->
                val status = member.rows("CALL dodeca.cluster.status()", "system")
                status.single { it["database"] == "system" }["role"] == "LEADER"
            }
            assertThat(leader.run("GRANT ROLE admin TO neo4j", "system"))
                .isInstanceOf(Outcome.Rows::class.java)
        }
        restart()

        val recovered = driver(AuthTokens.basic("neo4j", DodecaContainer.PASSWORD))
        eventually {
            recovered.system("CREATE USER carol SET PASSWORD 'carol-secret-1' CHANGE NOT REQUIRED")
        }
        for (member in members) {
            eventually {
                assertThat(member.runAs("carol", "carol-secret-1", "CALL db.info() YIELD name"))
                    .isEqualTo(Outcome.Rows(listOf(mapOf("name" to "neo4j"))))
            }
        }
    }

    /** Stops every member and starts it again on its data with the environment [settings]. */
    private fun restart(settings: Map<String, String> = emptyMap()) {
        drivers.forEach(Driver::close)
        drivers.clear()
        members.forEach {
            it.shutDown()
            it.saveLogs(logs())
            it.stop()
        }
        restarts++
        members = members(settings)
        Startables.deepStart(members).join()
    }
}
