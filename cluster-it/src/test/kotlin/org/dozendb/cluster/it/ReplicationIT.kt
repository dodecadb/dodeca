package org.dozendb.cluster.it

import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
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
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/** Three members replicate the default database; the leader's writes reach every member. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class ReplicationIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    // Fixed host ports: Docker may publish a restarted container on other ports.
    private val members = aliases.map { alias ->
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(TestPorts.free())
    }
    private val running = members.toMutableList()
    private val acknowledged = mutableListOf<Int>()

    @BeforeAll
    fun start() {
        Startables.deepStart(members).join()
    }

    @AfterAll
    fun stop() {
        members.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun role(member: DozenDbContainer): Any? =
        when (val outcome = member.run("CALL dozendb.cluster.status()", "system")) {
            is Outcome.Rows -> outcome.rows.singleOrNull { it["database"] == "neo4j" }?.get("role")
            is Outcome.Failed -> null
        }

    private fun awaitLeader(): DozenDbContainer =
        await.pollDelay(Duration.ZERO) withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilNotNull
            {
                running.filter { role(it) == "LEADER" }.singleOrNull()
            }

    private fun count(member: DozenDbContainer, label: String): Long =
        member.rows("MATCH (n:$label) RETURN count(n) AS c").single()["c"] as Long

    /** The whole graph with record IDs; members that applied the same log return the same. */
    private fun contents(member: DozenDbContainer): List<Map<String, Any?>> =
        member.rows(
            "MATCH (n) OPTIONAL MATCH (n)-[r]->(m) RETURN id(n) AS n, labels(n) AS labels, " +
                "properties(n) AS props, id(r) AS r, type(r) AS type, id(m) AS m, properties(r) AS relProps " +
                "ORDER BY n, r"
        )

    private fun awaitIdentical() {
        await atMost
            Duration.ofSeconds(30) untilAsserted
            {
                val expected = contents(running.first())
                running.drop(1).forEach {
                    assertThat(contents(it)).describedAs(it.alias).isEqualTo(expected)
                }
            }
    }

    @Test
    @Order(1)
    fun `the leader's writes reach every member and the stores match`() {
        val leader = awaitLeader()

        leader.rows(
            "UNWIND range(1, 100) AS i CREATE (:Item {i: i})-[:NEXT {i: i}]->(:Tail {i: i})"
        )
        leader.rows("MATCH (t:Tail) WHERE t.i % 4 = 0 DETACH DELETE t")

        awaitIdentical()
        members.forEach { assertThat(count(it, "Item")).isEqualTo(100) }
        val databaseIds = members.map { it.rows("CALL db.info() YIELD id").single()["id"] }
        assertThat(databaseIds.distinct()).hasSize(1)
    }

    @Test
    @Order(2)
    fun `followers reject writes with a not-a-leader error`() {
        val leader = awaitLeader()
        val follower = running.first { it != leader }

        val outcome = follower.run("CREATE (:Rejected)")

        assertThat(outcome).isInstanceOfSatisfying(Outcome.Failed::class.java) {
            assertThat(it.code).isEqualTo("Neo.ClientError.Cluster.NotALeader")
        }
        assertThat(count(leader, "Rejected")).isZero()
    }

    @Test
    @Order(3)
    fun `existence constraints work in clustered mode`() {
        val leader = awaitLeader()

        leader.rows("CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS NOT NULL")
        leader.rows("CREATE (:Person {name: 'ada'})")
        val violation = leader.run("CREATE (:Person {age: 3})")

        assertThat(violation).isInstanceOfSatisfying(Outcome.Failed::class.java) {
            assertThat(it.code).isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
        }
        awaitIdentical()
        members.forEach { member ->
            val names = member.rows("SHOW CONSTRAINTS YIELD name").map { it["name"] }
            assertThat(names).describedAs(member.alias).contains("person_name")
        }
    }

    @Test
    @Order(4)
    fun `every acknowledged write survives killing the leader during writes`() {
        var killedAt = -1
        var next = 0
        val deadline = System.nanoTime() + Duration.ofMinutes(2).toNanos()
        // One write per transaction; after the leader dies, keep writing to whoever leads next.
        while (acknowledged.count { it > killedAt } < 50 || killedAt < 0) {
            check(System.nanoTime() < deadline) { "only ${acknowledged.size} writes acknowledged" }
            val leader = awaitLeader()
            val n = next++
            if (leader.run("CREATE (:Ack {n: $n})") is Outcome.Rows) acknowledged += n
            if (killedAt < 0 && acknowledged.size == 50) {
                killedAt = n
                leader.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
                leader.kill()
                running -= leader
            }
        }

        awaitIdentical()
        assertAcknowledgedOnEveryMember()
    }

    @Test
    @Order(5)
    fun `the killed leader restarts and catches up with what it missed`() {
        val killed = members.single { it !in running }
        val n = acknowledged.max() + 1
        awaitLeader().rows("CREATE (:Ack {n: $n})")
        acknowledged += n

        killed.restart()
        running += killed

        awaitIdentical()
        assertAcknowledgedOnEveryMember()
    }

    private fun assertAcknowledgedOnEveryMember() {
        running.forEach { member ->
            val stored =
                member.rows("MATCH (a:Ack) RETURN a.n AS n").map { (it["n"] as Long).toInt() }
            assertThat(stored)
                .describedAs(member.alias)
                .containsAll(acknowledged)
                .doesNotHaveDuplicates()
        }
    }
}
