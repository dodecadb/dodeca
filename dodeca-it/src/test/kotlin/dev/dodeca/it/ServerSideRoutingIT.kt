package dev.dodeca.it

import dev.dodeca.consensus.testing.TestAuthority
import dev.dodeca.consensus.testing.TestPorts
import java.nio.file.Path
import java.time.Duration
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.junit.jupiter.api.io.TempDir
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.ClientCertificateManagers
import org.neo4j.driver.ClientCertificates
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.AuthenticationException
import org.neo4j.driver.exceptions.Neo4jException
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * With server-side routing, a driver that knows one member writes through it: members forward
 * writes to the leader's routing connector over mutual TLS with hostname verification. No
 * acknowledged write is lost or duplicated when the leader is cut off or dies under load.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ServerSideRoutingIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
            .withHttp()
            .withEnv("NEO4J_dbms_routing_default__router", "SERVER")
            .apply { addExposedPort(DodecaContainer.ROUTING_PORT) }
    }
    private val running = members.toMutableList()
    private val faults = mutableListOf<NetworkFaults>()
    private val drivers = mutableListOf<Driver>()

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
    }

    @AfterEach
    fun heal() {
        faults.forEach { it.close() }
        faults.clear()
        drivers.forEach { it.close() }
        drivers.clear()
    }

    @AfterAll
    fun stop() {
        members.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun role(member: DodecaContainer, database: String = "neo4j"): Any? =
        when (val outcome = member.run("CALL dodeca.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }?.get("role")
            is Outcome.Failed -> null
        }

    private fun awaitLeader(database: String = "neo4j"): DodecaContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                running.singleOrNull { role(it, database) == "LEADER" }
            }

    /** A driver that routes on the server, through [member] alone. */
    private fun routing(member: DodecaContainer): Driver =
        GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts[members.indexOf(member)]}",
                AuthTokens.basic("neo4j", DodecaContainer.PASSWORD),
            )
            .also { drivers += it }

    private fun count(member: DodecaContainer, query: String, database: String = "neo4j") =
        member.rows(query, database).single().values.single() as Long

    @Test
    @Order(1)
    fun `writes, administration commands and HTTP requests through a follower run on the leader`() {
        val leader = awaitLeader()
        val follower = running.first { it != leader }
        val driver = routing(follower)

        driver.session(SessionConfig.forDatabase("neo4j")).use { session ->
            session.executeWrite { tx -> tx.run("CREATE (:ViaBolt)").consume() }
            session
                .run("CYPHER 25 ALTER CURRENT GRAPH TYPE ADD { (:Routed => {n :: INTEGER}) }")
                .consume()
        }
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            session.run("CREATE DATABASE films WAIT").consume()
        }
        driver.session(SessionConfig.forDatabase("films")).use { session ->
            session.executeWrite { tx -> tx.run("CREATE (:Film)").consume() }
        }
        val queryApi =
            follower.post("/db/neo4j/query/v2", """{"statement": "CREATE (:ViaQueryApi)"}""")
        val transactionApi =
            follower.post(
                "/db/neo4j/tx/commit",
                """{"statements": [{"statement": "CREATE (:ViaTransactionApi)"}]}""",
            )

        assertThat(queryApi.statusCode()).describedAs(queryApi.body()).isBetween(200, 299)
        assertThat(transactionApi.statusCode()).isEqualTo(200)
        assertThat(transactionApi.body()).contains("\"errors\":[]")
        for (member in running) {
            await atMost
                Duration.ofSeconds(30) untilAsserted
                {
                    assertThat(
                            count(
                                member,
                                "MATCH (n) WHERE n:ViaBolt OR n:ViaQueryApi OR " +
                                    "n:ViaTransactionApi RETURN count(n)",
                            )
                        )
                        .describedAs(member.alias)
                        .isEqualTo(3)
                    assertThat(count(member, "MATCH (n:Film) RETURN count(n)", "films"))
                        .describedAs(member.alias)
                        .isEqualTo(1)
                    assertThat(
                            member
                                .rows("CYPHER 25 SHOW CURRENT GRAPH TYPE")
                                .single()["specification"]
                                as String
                        )
                        .describedAs(member.alias)
                        .contains("(:`Routed` => {`n` :: INTEGER})")
                }
        }
    }

    @Test
    @Order(2)
    fun `the routing connector takes logins of members only`(@TempDir directory: Path) {
        val member = running.first()
        val trusted = directory.resolve("cluster")
        DodecaContainer.clusterAuthority.issue(names = listOf("client")).writeTo(trusted)
        val stranger = directory.resolve("stranger")
        TestAuthority("Stranger CA").issue(names = listOf("client")).writeTo(stranger)
        fun login(certificate: Path?, auth: AuthToken): Throwable? {
            val trust =
                Config.TrustStrategy.trustCustomCertificateSignedBy(
                        trusted.resolve("trusted/authority-0.crt").toFile()
                    )
                    .withoutHostnameVerification()
            val config = Config.builder().withEncryption().withTrustStrategy(trust).build()
            val uri = "bolt://${member.host}:${member.getMappedPort(DodecaContainer.ROUTING_PORT)}"
            val driver =
                if (certificate == null) GraphDatabase.driver(uri, auth, config)
                else
                    GraphDatabase.driver(
                        uri,
                        auth,
                        ClientCertificateManagers.rotating(
                            ClientCertificates.of(
                                certificate.resolve("public.crt").toFile(),
                                certificate.resolve("private.key").toFile(),
                            )
                        ),
                        config,
                    )
            return catchThrowable { driver.use { it.verifyConnectivity() } }
        }
        val memberLogin = AuthTokens.custom("neo4j", "", null, "dodeca-member")

        assertThat(login(trusted, memberLogin)).isNull()
        assertThat(login(trusted, AuthTokens.basic("neo4j", DodecaContainer.PASSWORD)))
            .isInstanceOf(AuthenticationException::class.java)
            .hasMessageContaining("takes logins of cluster members only")
        assertThat(login(stranger, memberLogin)).isNotNull()
        assertThat(login(null, memberLogin)).isNotNull()
    }

    /**
     * Writes nodes with [label] and an `id` through [driver] from [threads] threads, each id in one
     * managed transaction, until [stop].
     */
    private class Load(driver: Driver, val label: String, threads: Int = 4) {
        /** Ids whose transaction committed. */
        val acknowledged = ConcurrentLinkedQueue<Long>()

        /** Ids whose transaction may or may not have committed. */
        val unknown = ConcurrentLinkedQueue<Long>()

        /** Ids whose transaction failed otherwise, with the failure. */
        val failed = ConcurrentLinkedQueue<Pair<Long, Throwable>>()

        private val ids = AtomicLong()
        private val writing = AtomicBoolean(true)
        private val workers =
            (1..threads).map {
                thread(name = "load-$it") {
                    driver.session(SessionConfig.forDatabase("neo4j")).use { session ->
                        while (writing.get()) {
                            val id = ids.incrementAndGet()
                            try {
                                session.executeWrite { tx ->
                                    tx.run("CREATE (:$label {id: \$id})", mapOf("id" to id))
                                        .consume()
                                }
                                acknowledged += id
                            } catch (e: Neo4jException) {
                                if (e.code() == COMMIT_FAILED) unknown += id else failed += id to e
                            }
                        }
                    }
                }
            }

        /** Waits until [more] further transactions committed. */
        fun awaitAcknowledged(more: Int) {
            val target = acknowledged.size + more
            await withPollInterval
                Duration.ofMillis(100) atMost
                Duration.ofMinutes(2) untilAsserted
                {
                    assertThat(acknowledged.size).isGreaterThanOrEqualTo(target)
                }
        }

        fun stop() {
            writing.set(false)
            workers.forEach { it.join() }
        }

        private companion object {
            const val COMMIT_FAILED = "Neo.DatabaseError.Transaction.TransactionCommitFailed"
        }
    }

    /**
     * Checks that every running member has every acknowledged write once, and no write that failed
     * for sure.
     */
    private fun assertWritten(load: Load) {
        println(
            "${load.label}: ${load.acknowledged.size} writes acknowledged, " +
                "${load.unknown.size} with an unknown outcome, ${load.failed.size} failed"
        )
        assertThat(load.failed).describedAs("writes that failed").isEmpty()
        val possible = load.acknowledged + load.unknown
        val query = "MATCH (n:${load.label}) RETURN n.id AS id"
        for (member in running) {
            await withPollInterval
                Duration.ofMillis(500) atMost
                Duration.ofMinutes(2) untilAsserted
                {
                    val ids = member.rows(query).map { it["id"] }
                    assertThat(ids).describedAs(member.alias).containsAll(load.acknowledged)
                }
            val ids = member.rows(query).map { it["id"] as Long }
            assertThat(ids).describedAs(member.alias).doesNotHaveDuplicates()
            assertThat(possible).describedAs(member.alias).containsAll(ids)
        }
    }

    @Test
    @Order(3)
    fun `writes through a follower go on when the leader is cut off from the others`() {
        val leader = awaitLeader()
        val follower = running.first { it != leader }
        val load = Load(routing(follower), "CutOff")
        try {
            load.awaitAcknowledged(50)
            NetworkFaults(leader).also { faults += it }.cutRaft()
            load.awaitAcknowledged(100)
            faults.forEach { it.healRaft() }
            load.awaitAcknowledged(50)
        } finally {
            load.stop()
        }

        assertWritten(load)
    }

    @Test
    @Order(4)
    fun `writes through a follower go on when the leader dies`() {
        val leader = awaitLeader()
        val follower = running.first { it != leader }
        val load = Load(routing(follower), "Died")
        try {
            load.awaitAcknowledged(50)
            leader.kill()
            running -= leader
            load.awaitAcknowledged(100)
        } finally {
            load.stop()
        }

        assertWritten(load)
    }
}
