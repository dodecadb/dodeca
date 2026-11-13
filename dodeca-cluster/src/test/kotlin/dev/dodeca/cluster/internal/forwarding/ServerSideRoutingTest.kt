package dev.dodeca.cluster.internal.forwarding

import dev.dodeca.cluster.internal.ClusterHarness
import dev.dodeca.cluster.internal.ClusterSettings
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.untilNotNull
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.Query
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.TransactionConfig
import org.neo4j.driver.Value
import org.neo4j.driver.Values
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.summary.ResultSummary

/**
 * Write transactions of clients that allow routing on the server, which reach a member that does
 * not lead their database, run on its leader as their user, with the same results.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ServerSideRoutingTest {
    private lateinit var cluster: ClusterHarness
    private val drivers = HashMap<String, Driver>()

    @BeforeAll
    fun start(
        // Kept when a test fails, for the members' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        cluster =
            ClusterHarness(
                root,
                3,
                settings =
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                        // Routing tables name only the member a driver connects to.
                        GraphDatabaseSettings.routing_default_router to
                            GraphDatabaseSettings.RoutingMode.SERVER,
                    ),
            )
        cluster.start()
        administer("ALTER USER neo4j SET PASSWORD '$ADMIN_PASSWORD' CHANGE NOT REQUIRED")
        createUser("alice", "publisher")
        createUser("bob", "reader")
        createUser("carol", "publisher")
        administer("CREATE ROLE impersonator")
        administer("GRANT IMPERSONATE (alice) ON DBMS TO impersonator")
        administer("GRANT ROLE impersonator TO carol")
        administer("CREATE DATABASE films WAIT")
        awaitEverywhere("SHOW DATABASES YIELD name WHERE name = 'films' RETURN DISTINCT name", 1)
    }

    @AfterAll
    fun stop() {
        drivers.values.forEach(Driver::close)
        cluster.close()
    }

    private fun administer(command: String) {
        cluster.awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    /** Waits until [query] on `system` has [rows] rows on every member. */
    private fun awaitEverywhere(query: String, rows: Long) {
        for (member in cluster.members) {
            await withPollInterval
                Duration.ofMillis(100) atMost
                ClusterHarness.AWAIT untilAsserted
                {
                    assertThat(
                            member.database(SYSTEM).executeTransactionally(query, emptyMap()) {
                                it.stream().count()
                            }
                        )
                        .isEqualTo(rows)
                }
        }
    }

    private fun createUser(user: String, role: String) {
        administer("CREATE USER $user SET PASSWORD '${password(user)}' CHANGE NOT REQUIRED")
        administer("GRANT ROLE $role TO $user")
        awaitEverywhere(
            "SHOW USERS YIELD user, roles WHERE user = '$user' AND '$role' IN roles RETURN user",
            1,
        )
    }

    private fun password(user: String) = if (user == "neo4j") ADMIN_PASSWORD else "$user-secret-1"

    /** A driver that routes on the server, at [member]. */
    private fun routing(member: ClusterHarness.Member, user: String = "alice"): Driver =
        driver("neo4j", member, user)

    /** A driver of [member] alone. */
    private fun direct(member: ClusterHarness.Member, user: String = "alice"): Driver =
        driver("bolt", member, user)

    private fun driver(scheme: String, member: ClusterHarness.Member, user: String): Driver =
        drivers.getOrPut("$scheme $member $user") {
            GraphDatabase.driver(
                "$scheme://${member.boltAddress()}",
                AuthTokens.basic(user, password(user)),
            )
        }

    private fun follower(database: String = NEO4J): ClusterHarness.Member {
        val leader = cluster.awaitLeader(database)
        return cluster.members.first { it != leader }
    }

    private fun count(
        member: ClusterHarness.Member,
        query: String,
        database: String = NEO4J,
    ): Long =
        member.database(database).executeTransactionally(query, emptyMap()) {
            it.next().values.single() as Long
        }

    @Test
    fun `writes through a follower run on the leader, auto-commit, explicit and managed`() {
        val follower = follower()
        val alice = routing(follower)
        val bookmarks =
            alice.session(SessionConfig.forDatabase(NEO4J)).use { session ->
                val autoCommit =
                    session
                        .run("CREATE (f:Film {title: 'Alien', form: 'auto'}) RETURN f.title AS t")
                        .list()
                assertThat(autoCommit.single()["t"].asString()).isEqualTo("Alien")
                session.beginTransaction().use { tx ->
                    tx.run("CREATE (:Film {title: 'Aliens', form: 'explicit'})").consume()
                    tx.commit()
                }
                val managed = session.executeWrite { tx ->
                    tx.run("CREATE (:Film {title: 'Alien 3', form: 'managed'})")
                        .consume()
                        .counters()
                        .nodesCreated()
                }
                assertThat(managed).isEqualTo(1)
                session.lastBookmarks()
            }
        // The bookmark makes the follower wait for the writes.
        val read =
            alice
                .session(
                    SessionConfig.builder().withDatabase(NEO4J).withBookmarks(bookmarks).build()
                )
                .use { session ->
                    session.executeRead { tx ->
                        tx.run("MATCH (f:Film) WHERE f.form IS NOT NULL RETURN count(f) AS c")
                            .single()["c"]
                            .asLong()
                    }
                }
        assertThat(read).isEqualTo(3)
        val leader = cluster.awaitLeader()
        assertThat(Files.readString(leader.home.resolve("logs/query.log")))
            .contains("dodeca-forwarding")
            .contains("alice - CREATE (:Film {title: 'Aliens', form: 'explicit'})")
    }

    @Test
    fun `linearizable transactions through a follower wait on the leader, reads where they run`() {
        val leader = cluster.awaitLeader()
        val (follower, other) = cluster.members.filter { it != leader }
        val linearizable =
            TransactionConfig.builder()
                .withMetadata(mapOf("dodeca.consistency" to "linearizable"))
                .build()
        routing(follower).session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session.executeWrite({ tx -> tx.run("CREATE (:Linearizable)").consume() }, linearizable)
        }

        // No bookmark: the read waits for what the leader had committed when it began.
        val read =
            direct(other).session(SessionConfig.forDatabase(NEO4J)).use { session ->
                session
                    .run("MATCH (n:Linearizable) RETURN count(n) AS c", linearizable)
                    .single()["c"]
                    .asLong()
            }
        assertThat(read).isEqualTo(1)
        val unknown =
            TransactionConfig.builder()
                .withMetadata(mapOf("dodeca.consistency" to "strong"))
                .build()
        val refused =
            catchThrowable {
                routing(follower).session(SessionConfig.forDatabase(NEO4J)).use {
                    it.run("CREATE (:Strong)", unknown).consume()
                }
            }
                as Neo4jException
        assertThat(refused.code()).isEqualTo("Neo.ClientError.Request.Invalid")
    }

    @Test
    fun `results through a follower are those of the leader`() {
        val leader = cluster.awaitLeader()
        val follower = follower()
        direct(leader).session(SessionConfig.forDatabase(NEO4J)).use {
            it.run(
                    "CREATE (a:Person {name: 'Ann', born: date('1970-01-01')})" +
                        "-[:KNOWS {since: datetime('2020-01-01T10:00:00[Europe/Stockholm]')}]->" +
                        "(b:Person {name: 'Ben', at: point({longitude: 18.07, latitude: 59.33})})"
                )
                .consume()
        }
        val queries =
            listOf(
                Query(
                    "RETURN 1 AS one, 2.5 AS half, 'x' AS s, true AS t, null AS n, [1, 'a'] AS l"
                ),
                Query(
                    "RETURN {a: 1, b: [true, null]} AS m, \$bytes AS bytes",
                    Values.parameters("bytes", byteArrayOf(1, 2, 3)),
                ),
                Query(
                    "RETURN date('2024-02-29') AS d, time('12:00:00+01:00') AS t, " +
                        "localtime('12:00') AS lt, localdatetime('2024-01-01T00:00') AS ldt, " +
                        "datetime('2024-10-27T02:30:00[Europe/Stockholm]') AS dt, " +
                        "duration('P1Y2M3DT4H5M6.007S') AS du, point({x: 1, y: 2, z: 3}) AS p"
                ),
                Query("MATCH p = (a:Person)-[r:KNOWS]->(b) RETURN a, r, b, p"),
                Query("UNWIND range(1, 2500) AS i RETURN i"),
                Query("MATCH (n:NoSuchLabel) RETURN n"),
                Query("CREATE (x:Scratch {v: \$v}) RETURN x.v AS v", Values.parameters("v", 42)),
                Query("EXPLAIN MATCH (p:Person) RETURN p"),
                Query("PROFILE MATCH (p:Person) RETURN p.name AS name ORDER BY name"),
            )
        for (query in queries) {
            val direct = run(direct(leader), query)
            val forwarded = run(routing(follower), query)
            assertThat(forwarded.first).describedAs(query.text()).isEqualTo(direct.first)
            val (expected, actual) = direct.second to forwarded.second
            assertThat(actual.counters().nodesCreated())
                .isEqualTo(expected.counters().nodesCreated())
            assertThat(actual.queryType()).isEqualTo(expected.queryType())
            assertThat(actual.gqlStatusObjects().map { it.gqlStatus() })
                .describedAs(query.text())
                .isEqualTo(expected.gqlStatusObjects().map { it.gqlStatus() })
            @Suppress("DEPRECATION")
            assertThat(actual.notifications().map { it.code() })
                .isEqualTo(expected.notifications().map { it.code() })
            assertThat(actual.queryPlan().map { it.operatorType() })
                .describedAs(query.text())
                .isEqualTo(expected.queryPlan().map { it.operatorType() })
            assertThat(actual.queryProfile().isPresent).isEqualTo(expected.queryProfile().isPresent)
        }
    }

    private fun run(driver: Driver, query: Query): Pair<List<Map<String, Any?>>, ResultSummary> =
        driver.session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session.executeWrite { tx ->
                val result = tx.run(query)
                result.list { record -> record.asMap { content(it) } } to result.consume()
            }
        }

    /** What [value] holds, compared by content: nodes with their ids, labels and properties. */
    private fun content(value: Value): Any? =
        when (value.type().name()) {
            "NODE" ->
                value.asNode().let {
                    listOf(it.elementId(), it.labels().toList(), it.asMap { p -> content(p) })
                }
            "RELATIONSHIP" ->
                value.asRelationship().let {
                    listOf(
                        it.elementId(),
                        it.startNodeElementId(),
                        it.endNodeElementId(),
                        it.type(),
                        it.asMap { p -> content(p) },
                    )
                }
            "PATH" ->
                value.asPath().let { path ->
                    path.nodes().map { content(Values.value(it)) } +
                        path.relationships().map { content(Values.value(it)) }
                }
            "BYTES" -> value.asByteArray().toList()
            "LIST" -> value.asList { content(it) }
            "MAP" -> value.asMap { content(it) }
            else -> value.asObject()
        }

    @Test
    fun `failures on the leader reach the client as they are`() {
        val leader = cluster.awaitLeader()
        val follower = follower()
        direct(leader, "neo4j").session(SessionConfig.forDatabase(NEO4J)).use {
            it.run(
                    "CREATE CONSTRAINT unique_isbn IF NOT EXISTS FOR (b:Book) REQUIRE b.isbn IS UNIQUE"
                )
                .consume()
            it.run("MERGE (:Book {isbn: '1'})").consume()
        }
        for (query in
            listOf(
                "CREATE (:Book {isbn: '1'})",
                "RETURN 1 / 0",
                "CREAT (n)",
                "RETURN no.such.function()",
            )) {
            val direct = catchThrowable { run(direct(leader), Query(query)) } as Neo4jException
            val forwarded =
                catchThrowable { run(routing(follower), Query(query)) } as Neo4jException
            assertThat(forwarded).describedAs(query).isInstanceOf(direct.javaClass)
            assertThat(forwarded.code()).describedAs(query).isEqualTo(direct.code())
            assertThat(forwarded.gqlStatus()).describedAs(query).isEqualTo(direct.gqlStatus())
            assertThat(forwarded.message).describedAs(query).isEqualTo(direct.message)
        }
    }

    @Test
    fun `administration commands through a follower run on the system leader`() {
        val follower = follower(SYSTEM)
        routing(follower, "neo4j").session(SessionConfig.forDatabase(SYSTEM)).use { session ->
            session.run("CREATE USER dave SET PASSWORD 'dave-secret-1' CHANGE REQUIRED").consume()
            session.run("CREATE DATABASE books WAIT").consume()
        }
        // Every member shows the copy on every member online.
        awaitEverywhere(
            "SHOW DATABASES YIELD name, currentStatus WHERE name = 'books' AND currentStatus = 'online' RETURN name",
            cluster.members.size.toLong(),
        )
        // A user who must change their password changes it through a follower too.
        awaitEverywhere("SHOW USERS YIELD user WHERE user = 'dave' RETURN user", 1)
        GraphDatabase.driver(
                "neo4j://${follower.boltAddress()}",
                AuthTokens.basic("dave", "dave-secret-1"),
            )
            .use { dave ->
                dave.session(SessionConfig.forDatabase(SYSTEM)).use {
                    it.run(
                            "ALTER CURRENT USER SET PASSWORD FROM 'dave-secret-1' TO 'dave-secret-2'"
                        )
                        .consume()
                }
            }
        awaitEverywhere(
            "SHOW USERS YIELD user, passwordChangeRequired WHERE user = 'dave' AND NOT passwordChangeRequired RETURN user",
            1,
        )
    }

    @Test
    fun `bolt clients write on a follower only when the setting forwards them`() {
        // A member that leads neither database: its write transactions on both are forwarded.
        val follower =
            cluster.members.first {
                it != cluster.awaitLeader(NEO4J) && it != cluster.awaitLeader(SYSTEM)
            }
        val admin = direct(follower, "neo4j")
        fun forwards() =
            follower
                .database(SYSTEM)
                .dependencyResolver
                .resolveDependency(Config::class.java)
                .get(ClusterSettings.forwardBoltWrites)
        fun write() =
            direct(follower).session(SessionConfig.forDatabase(NEO4J)).use {
                it.run("CREATE (:Bolt)").consume()
            }

        val refused = catchThrowable { write() } as Neo4jException
        assertThat(refused.code()).isEqualTo("Neo.ClientError.Cluster.NotALeader")
        assertThat(refused.message)
            .contains("leads database 'neo4j', not this member")
            .contains("Bolt clients (bolt://) write on the leader, or through any member with")
            .contains("dbms.dodeca.routing.forward_bolt_writes=true")

        // In read transactions, which run where they reach, as the setting changes only there.
        fun configure(value: Boolean) =
            admin
                .session(
                    SessionConfig.builder()
                        .withDatabase(SYSTEM)
                        .withDefaultAccessMode(AccessMode.READ)
                        .build()
                )
                .use {
                    it.run(
                            "CALL dbms.setConfigValue(" +
                                "'dbms.dodeca.routing.forward_bolt_writes', '$value')"
                        )
                        .consume()
                }
        configure(true)
        assertThat(forwards()).isTrue()
        try {
            write()
            assertThat(count(cluster.awaitLeader(), "MATCH (b:Bolt) RETURN count(b)")).isEqualTo(1)
        } finally {
            configure(false)
        }
        assertThat(forwards()).describedAs("the setting on the member it was changed on").isFalse()
    }

    @Test
    fun `a transaction writes to a database that another member leads`() {
        val filmsLeader = cluster.awaitLeader(FILMS)
        val member = cluster.members.first { it != filmsLeader }
        routing(member).session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session.run("USE films CREATE (:Remake {title: 'Solaris'})").consume()
        }
        assertThat(count(filmsLeader, "MATCH (r:Remake) RETURN count(r)", FILMS)).isEqualTo(1)
    }

    @Test
    fun `users act as others through a follower, with their privileges`() {
        val follower = follower()
        routing(follower, "carol")
            .session(
                SessionConfig.builder().withDatabase(NEO4J).withImpersonatedUser("alice").build()
            )
            .use { session ->
                val user = session.executeWrite { tx ->
                    tx.run(
                            "CREATE (:Impersonated) WITH 1 AS x " +
                                "CALL dbms.showCurrentUser() YIELD username RETURN username"
                        )
                        .single()["username"]
                        .asString()
                }
                assertThat(user).isEqualTo("alice")
            }
        // bob may only read, also on the leader.
        val refused =
            catchThrowable {
                routing(follower, "bob").session(SessionConfig.forDatabase(NEO4J)).use {
                    it.run("CREATE (:Refused)").consume()
                }
            }
                as Neo4jException
        assertThat(refused.code()).isEqualTo("Neo.ClientError.Security.Forbidden")
    }

    @Test
    fun `timeouts and terminations reach the transaction on the leader`() {
        val leader = cluster.awaitLeader()
        val follower = follower()
        val longQuery = "UNWIND range(1, 2000000000) AS i RETURN count(i)"
        val timedOut =
            catchThrowable {
                routing(follower).session(SessionConfig.forDatabase(NEO4J)).use {
                    it.run(
                            longQuery,
                            TransactionConfig.builder().withTimeout(Duration.ofSeconds(1)).build(),
                        )
                        .consume()
                }
            }
                as Neo4jException
        assertThat(timedOut.code()).startsWith("Neo.ClientError.Transaction.TransactionTimedOut")

        var terminated: Throwable? = null
        val running = thread {
            terminated = catchThrowable {
                routing(follower).session(SessionConfig.forDatabase(NEO4J)).use {
                    it.executeWrite { tx -> tx.run(longQuery).consume() }
                }
            }
        }
        val id =
            await withPollInterval
                Duration.ofMillis(100) atMost
                ClusterHarness.AWAIT untilNotNull
                {
                    leader.database(SYSTEM).executeTransactionally(
                        "SHOW TRANSACTIONS YIELD transactionId, currentQuery, username " +
                            "WHERE currentQuery STARTS WITH 'UNWIND range(1, 2000000000)' RETURN transactionId, username",
                        emptyMap(),
                    ) {
                        it.stream()
                            .findFirst()
                            .map { row ->
                                assertThat(row["username"]).isEqualTo("alice")
                                row["transactionId"] as String
                            }
                            .orElse(null)
                    }
                }
        leader.database(SYSTEM).executeTransactionally("TERMINATE TRANSACTION '$id'")
        running.join(ClusterHarness.AWAIT.toMillis())
        assertThat((terminated as Neo4jException).code())
            .isEqualTo("Neo.ClientError.Transaction.Terminated")
    }

    @Test
    fun `explicit transactions begin with BEGIN, also through a follower`() {
        val leader = cluster.awaitLeader()
        val warnings = { member: ClusterHarness.Member ->
            Files.readAllLines(member.home.resolve("logs/debug.log")).count {
                it.contains("Could not eagerly create kernel transaction")
            }
        }
        val before = cluster.members.associateWith(warnings)

        // As in Neo4j, it shows on the leader before its first query.
        direct(leader).session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session
                .beginTransaction(
                    TransactionConfig.builder().withMetadata(mapOf("begun" to "idle")).build()
                )
                .use { tx ->
                    await withPollInterval
                        Duration.ofMillis(100) atMost
                        ClusterHarness.AWAIT untilAsserted
                        {
                            assertThat(
                                    count(
                                        leader,
                                        "SHOW TRANSACTIONS YIELD metaData " +
                                            "WHERE metaData.begun = 'idle' RETURN count(*)",
                                        SYSTEM,
                                    )
                                )
                                .isEqualTo(1)
                        }
                    tx.run("CREATE (:Begun)").consume()
                    tx.commit()
                }
        }
        // Through a follower, it begins on the leader with its first query.
        routing(follower()).session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session.beginTransaction().use { tx ->
                tx.run("CREATE (:Begun)").consume()
                tx.commit()
            }
        }

        assertThat(count(leader, "MATCH (b:Begun) RETURN count(b)")).isEqualTo(2)
        assertThat(cluster.members.associateWith(warnings)).isEqualTo(before)
    }

    @Test
    fun `transaction metadata reaches the leader`() {
        val leader = cluster.awaitLeader()
        val follower = follower()
        routing(follower).session(SessionConfig.forDatabase(NEO4J)).use { session ->
            session.executeWrite(
                { tx -> tx.run("CREATE (:Tagged)").consume() },
                TransactionConfig.builder()
                    .withMetadata(mapOf("app" to "inventory", "n" to 7L))
                    .build(),
            )
        }
        assertThat(Files.readString(leader.home.resolve("logs/query.log")))
            .contains("CREATE (:Tagged)")
            .contains("app: 'inventory'")
    }

    private companion object {
        const val SYSTEM = "system"
        const val NEO4J = "neo4j"
        const val FILMS = "films"
        const val ADMIN_PASSWORD = "admin-secret-1"
    }
}
