package org.dozendb.it

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.exceptions.ClientException
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** A `neo4j://` driver writes through the leader, reads from followers and survives failover. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class RoutingIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.map { TestPorts.free() }
    private val members = aliases.mapIndexed { index, alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts[index])
            .withHttp()
            .apply {
                // A load balancer that cannot log in asks this one.
                if (alias == "core3") {
                    withEnv("NEO4J_dbms_security_cluster__status__auth__enabled", "false")
                }
            }
    }
    private val running = members.toMutableList()
    private lateinit var driver: Driver

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
        driver = driverSeededAt(members.first())
    }

    /**
     * A routing driver whose URI names [seed]. Its resolver adds every member, as a DNS name with
     * one record per member does in production: the driver resolves the default database through
     * its seed, so a seed that names one member would fail once that member is gone.
     */
    private fun driverSeededAt(seed: DozenDbContainer): Driver =
        GraphDatabase.driver(
            "neo4j://${boltAddress(seed)}",
            AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
            Config.builder()
                .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                .build(),
        )

    @AfterAll
    fun stop() {
        driver.close()
        members.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun boltAddress(member: DozenDbContainer) =
        "127.0.0.1:${boltPorts[members.indexOf(member)]}"

    private fun role(member: DozenDbContainer, database: String = "neo4j"): Any? =
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == database }?.get("role")
            is Outcome.Failed -> null
        }

    private fun awaitLeader(database: String = "neo4j"): DozenDbContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                running.singleOrNull { role(it, database) == "LEADER" }
            }

    @Suppress("UNCHECKED_CAST")
    private fun routingTable(
        member: DozenDbContainer,
        database: String,
    ): Map<String, List<String>> {
        val row =
            member.rows("CALL dbms.routing.getRoutingTable({}, '$database')", "system").single()
        return (row["servers"] as List<Map<String, Any?>>).associate {
            it["role"] as String to (it["addresses"] as List<String>).sorted()
        }
    }

    private fun write(cypher: String, through: Driver = driver) =
        through.session().use { session ->
            session.executeWrite { tx -> tx.run(cypher).consume() }
            session.lastBookmarks()
        }

    @Test
    @Order(1)
    fun `routing tables send writes to the leader and reads to the other members`() {
        val leader = awaitLeader()
        // Members learn each other's Bolt addresses within a refresh interval.
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                members.forEach { member ->
                    val table = routingTable(member, "neo4j")
                    assertThat(table["WRITE"]).containsExactly(boltAddress(leader))
                    // As in Neo4j Enterprise, the leader takes no reads by default.
                    assertThat(table["READ"])
                        .containsExactlyInAnyOrderElementsOf((members - leader).map(::boltAddress))
                    assertThat(table["ROUTE"])
                        .containsExactlyInAnyOrderElementsOf(members.map(::boltAddress))
                }
            }
        val members = members.first().rows("CALL dozendb.cluster.members()", "system")
        assertThat(members.map { it["boltAddress"] })
            .containsExactlyInAnyOrderElementsOf(this.members.map(::boltAddress))
        // Administration commands go to the system leader.
        val systemLeader = awaitLeader("system")
        this.members.forEach { member ->
            assertThat(routingTable(member, "system")["WRITE"])
                .containsExactly(boltAddress(systemLeader))
        }
    }

    @Test
    @Order(2)
    fun `reads chained by bookmarks see the preceding write wherever they run`() {
        val readers = mutableSetOf<String>()

        repeat(100) { i ->
            val bookmarks = write("CREATE (:Causal {i: $i})")
            val config =
                SessionConfig.builder()
                    .withBookmarks(bookmarks)
                    .withDefaultAccessMode(AccessMode.READ)
                    .build()
            driver.session(config).use { session ->
                session.executeRead { tx ->
                    val result = tx.run("MATCH (n:Causal {i: $i}) RETURN count(n) AS c")
                    assertThat(result.single()["c"].asLong()).describedAs("read $i").isEqualTo(1)
                    readers += result.consume().server().address()
                }
            }
        }

        assertThat(readers).describedAs("servers that answered reads").hasSizeGreaterThan(1)
    }

    @Test
    @Order(3)
    fun `linearizable reads see every acknowledged write without bookmarks`() {
        val linearizable =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "linearizable"))
                .build()
        val readConfig = SessionConfig.builder().withDefaultAccessMode(AccessMode.READ).build()

        repeat(100) { i ->
            write("CREATE (:Linear {i: $i})")
            driver.session(readConfig).use { session ->
                val count =
                    session.executeRead(
                        { tx ->
                            tx.run("MATCH (n:Linear {i: $i}) RETURN count(n) AS c")
                                .single()["c"]
                                .asLong()
                        },
                        linearizable,
                    )
                assertThat(count).describedAs("read $i").isEqualTo(1)
            }
        }
    }

    @Test
    @Order(4)
    fun `an unknown read consistency is refused`() {
        val unknown =
            TransactionConfig.builder()
                .withMetadata(mapOf("dozendb.consistency" to "strong"))
                .build()

        val error = catchThrowable {
            driver.session().use { session ->
                session.executeRead({ tx -> tx.run("RETURN 1").consume() }, unknown)
            }
        }

        assertThat(error).isInstanceOfSatisfying(ClientException::class.java) {
            assertThat(it.code()).isEqualTo("Neo.ClientError.Request.Invalid")
        }
    }

    @Test
    @Order(5)
    fun `HTTP writes on a follower run on the leader`() {
        val leader = awaitLeader()
        val follower = running.first { it != leader }

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
                            member.rows(
                                "MATCH (n) WHERE n:ViaQueryApi OR n:ViaTransactionApi " +
                                    "RETURN count(n) AS c"
                            )
                        )
                        .describedAs(member.alias)
                        .containsExactly(mapOf("c" to 2L))
                }
        }
    }

    @Test
    @Order(6)
    fun `drivers write by an alias, and in a home database set to one, through any member`() {
        driver.session(SessionConfig.forDatabase("system")).use { session ->
            session.run("CREATE ALIAS movies FOR DATABASE neo4j").consume()
            session
                .run(
                    "CREATE USER mia SET PASSWORD 'mia-secret-1' CHANGE NOT REQUIRED SET HOME DATABASE movies"
                )
                .consume()
            session.run("GRANT ROLE publisher TO mia").consume()
        }
        // Members learn of the alias a moment after the leader of system.
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                members.forEach { member ->
                    assertThat(routingTable(member, "movies"))
                        .isEqualTo(routingTable(member, "neo4j"))
                }
            }

        members.forEach { member ->
            driverSeededAt(member).use { seeded ->
                seeded.session(SessionConfig.forDatabase("movies")).use { session ->
                    session.executeWrite { tx ->
                        tx.run(
                                "CREATE (:ByAlias {member: \$member})",
                                mapOf("member" to member.alias),
                            )
                            .consume()
                    }
                }
            }
        }
        GraphDatabase.driver(
                "neo4j://${boltAddress(members.first())}",
                AuthTokens.basic("mia", "mia-secret-1"),
                Config.builder()
                    .withResolver { boltPorts.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
            .use { mia ->
                mia.session().use { session ->
                    session.executeWrite { tx ->
                        tx.run("CREATE (:ByAlias {member: 'home'})").consume()
                    }
                }
            }

        assertThat(
                members
                    .first()
                    .rows("MATCH (n:ByAlias) RETURN n.member AS member ORDER BY member", "movies")
            )
            .containsExactly(
                mapOf("member" to "core1"),
                mapOf("member" to "core2"),
                mapOf("member" to "core3"),
                mapOf("member" to "home"),
            )
    }

    @Test
    @Order(7)
    fun `cluster status endpoints tell load balancers where to write and read`() {
        val ids = members.associateWith { it.memberId() }
        val json = ObjectMapper()

        // Again if the leader moves meanwhile, or a member does not know it yet.
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                val leader = awaitLeader()
                for (member in members) {
                    clusterStatusMatches(member, leader, ids, json)
                }
            }
        val core1 = members.first { it.alias == "core1" }
        assertThat(core1.get("/db/neo4j/cluster/available", authenticated = false).statusCode())
            .describedAs("without credentials, by default")
            .isEqualTo(401)
        val core3 = members.first { it.alias == "core3" }
        assertThat(core3.get("/db/neo4j/cluster/available", authenticated = false).statusCode())
            .describedAs("without credentials, where cluster_status_auth_enabled=false")
            .isEqualTo(200)
        assertThat(core3.get("/dbms/cluster/status", authenticated = false).statusCode())
            .isEqualTo(200)
    }

    /** Asserts what the cluster status endpoints of [member] say, [leader] leading `neo4j`. */
    private fun clusterStatusMatches(
        member: DozenDbContainer,
        leader: DozenDbContainer,
        ids: Map<DozenDbContainer, String>,
        json: ObjectMapper,
    ) {
        val leads = member == leader
        val answers =
            listOf("writable", "read-only", "available").associateWith {
                member.get("/db/neo4j/cluster/$it").let { r -> r.statusCode() to r.body() }
            }
        assertThat(answers)
            .describedAs(member.alias)
            .isEqualTo(
                mapOf(
                    "writable" to if (leads) 200 to "true" else 404 to "false",
                    "read-only" to if (leads) 404 to "false" else 200 to "true",
                    "available" to (200 to "true"),
                )
            )
        val status = member.get("/db/neo4j/cluster/status")
        assertThat(status.statusCode()).describedAs(member.alias).isEqualTo(200)
        assertThat(status.headers().firstValue("Content-Type")).hasValueSatisfying {
            assertThat(it).startsWith("application/json")
        }
        val fields = json.readTree(status.body())
        assertThat(fields["core"].asBoolean()).isTrue
        assertThat(fields["memberId"].asText()).isEqualTo(ids.getValue(member))
        assertThat(fields["leader"].asText()).isEqualTo(ids.getValue(leader))
        assertThat(fields["votingMembers"].map { it.asText() })
            .containsExactlyInAnyOrderElementsOf(ids.values)
        assertThat(fields["participatingInRaftGroup"].asBoolean()).isTrue
        assertThat(fields["isHealthy"].asBoolean()).isTrue
        assertThat(fields["lastAppliedRaftIndex"].asLong()).isPositive
        val sinceLeader =
            assertThat(fields["millisSinceLastLeaderMessage"]?.asLong())
                .describedAs("${member.alias}: milliseconds since it heard from the leader")
        if (leads) sinceLeader.isZero() else sinceLeader.isBetween(0, 5_000)
        val all = json.readTree(member.get("/dbms/cluster/status").body())
        assertThat(all.map { it["databaseName"].asText() })
            .describedAs(member.alias)
            .containsExactlyInAnyOrder("system", "neo4j")
        assertThat(
                all.single { it["databaseName"].asText() == "neo4j" }["databaseStatus"]["memberId"]
                    .asText()
            )
            .isEqualTo(ids.getValue(member))
        assertThat(member.get("/db/nothing/cluster/available").statusCode()).isEqualTo(404)
    }

    @Test
    @Order(8)
    fun `writes continue on a new leader soon after the leader dies`() {
        val leader = awaitLeader()
        val acknowledged = mutableListOf<Int>()
        // Seeded at the member about to die, so the driver must fall back to the other members.
        val failover =
            driverSeededAt(leader).use { seededAtLeader ->
                repeat(30) { n ->
                    write("CREATE (:Failover {n: $n})", seededAtLeader)
                    acknowledged += n
                }
                leader.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))

                val killed = System.nanoTime()
                leader.kill()
                running -= leader
                // Managed transactions retry until a new leader takes the write.
                write("CREATE (:Failover {n: 30})", seededAtLeader)
                val elapsed = Duration.ofNanos(System.nanoTime() - killed)
                acknowledged += 30
                // By an alias as well.
                seededAtLeader.session(SessionConfig.forDatabase("movies")).use { session ->
                    session.executeWrite { tx -> tx.run("CREATE (:Failover {n: 31})").consume() }
                }
                acknowledged += 31
                (32 until 60).forEach { n ->
                    write("CREATE (:Failover {n: $n})", seededAtLeader)
                    acknowledged += n
                }
                elapsed
            }

        println("First write after the leader died took $failover")
        assertThat(failover)
            .describedAs("time to the first write after the leader died")
            .isLessThan(Duration.ofSeconds(10))
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                running.forEach { member ->
                    val stored =
                        member.rows("MATCH (n:Failover) RETURN n.n AS n").map {
                            (it["n"] as Long).toInt()
                        }
                    assertThat(stored)
                        .describedAs(member.alias)
                        .containsExactlyInAnyOrderElementsOf(acknowledged)
                }
            }
    }
}
