package dev.dodeca.it

import dev.dodeca.consensus.testing.TestPorts
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
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
import org.neo4j.driver.AccessMode
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.net.ServerAddress
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * A member cut off from the others, whose Raft packets are dropped so that it answers nothing at
 * all, does not keep them from routing: a database created with WAIT takes writes at once through a
 * member that does not host it, which asks the others where it is without waiting for the one cut
 * off. Once the network heals, the member catches up within seconds, however long it was cut off.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class CutOffMemberIT {
    private val network = Network.newNetwork()
    private val aliases = listOf("core1", "core2", "core3")
    private val boltPorts = aliases.associateWith { TestPorts.free() }
    private val members = aliases.associateWith { alias ->
        DodecaContainer(DodecaContainer.dodecaImage, alias)
            .inNetwork(network)
            .clusterMember(aliases.map { "$it:7700" })
            .boltOnHostPort(boltPorts.getValue(alias))
    }
    private val drivers = mutableListOf<Driver>()
    private var faults: NetworkFaults? = null

    @BeforeAll
    fun start() {
        Startables.deepStart(members.values).join()
    }

    @AfterEach
    fun heal() {
        faults?.let {
            it.healRaft()
            it.close()
        }
        faults = null
    }

    @AfterAll
    fun stop() {
        drivers.forEach(Driver::close)
        members.values.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    /** A `neo4j://` driver that knows the members called [known] only. */
    private fun routingDriver(vararg known: String): Driver =
        GraphDatabase.driver(
                "neo4j://127.0.0.1:${boltPorts.getValue(known.first())}",
                AuthTokens.basic("neo4j", DodecaContainer.PASSWORD),
                Config.builder()
                    .withResolver {
                        known.map { ServerAddress.of("127.0.0.1", boltPorts.getValue(it)) }.toSet()
                    }
                    .build(),
            )
            .also { drivers += it }

    private fun Driver.administer(command: String): List<Map<String, Any?>> {
        val config =
            SessionConfig.builder()
                .withDatabase("system")
                .withDefaultAccessMode(AccessMode.WRITE)
                .build()
        return session(config).use { session -> session.run(command).list { it.asMap() } }
    }

    /** The role of [alias] in the group of each database that runs there, as it knows itself. */
    private fun roles(alias: String): Map<Any?, Any?> =
        members
            .getValue(alias)
            .rows("CALL dodeca.cluster.status() YIELD database, role", "system")
            .associate { it["database"] to it["role"] }

    /** The commit index of each database's group, as the members [of] know it at most. */
    private fun commits(of: Collection<DodecaContainer>): Map<Any?, Long> =
        of.flatMap { it.rows("CALL dodeca.cluster.status() YIELD database, commitIndex", "system") }
            .groupBy({ it["database"] }, { it["commitIndex"] as Long })
            .mapValues { (_, indexes) -> indexes.max() }

    /** The applied index of each database's group on [member]. */
    private fun applied(member: DodecaContainer): Map<Any?, Long> =
        member
            .rows("CALL dodeca.cluster.status() YIELD database, appliedIndex", "system")
            .associate { it["database"] to it["appliedIndex"] as Long }

    /** The member that leads the group of [database] among [of], once there is one. */
    private fun leaderOf(database: String, of: Collection<String>): DodecaContainer =
        await withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(60) untilNotNull
            {
                of.singleOrNull { roles(it)[database] == "LEADER" }?.let(members::getValue)
            }

    @Test
    @Order(1)
    fun `a database created with WAIT takes writes at once through a member that does not host it`() {
        val names = (1..3).map { "waited$it" }
        // Each database is placed on core1, so that core2 routes it without hosting it.
        for (alias in listOf("core2", "core3")) {
            val id = members.getValue(alias).memberId()
            routingDriver("core1")
                .administer(
                    "ALTER SERVER '$id' SET OPTIONS {deniedDatabases: ${names.map { "'$it'" }}}"
                )
        }
        faults = NetworkFaults(members.getValue("core3")).also { it.cutRaft() }
        // The others elect a leader of `system` if core3 led it.
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofSeconds(60) untilAsserted
            {
                assertThat(listOf("core1", "core2").map { roles(it)["system"] }).contains("LEADER")
            }
        // A driver that knows nothing yet: one that knew core3 as the leader of `system` would send
        // the next command there, without retrying.
        val administration = routingDriver("core1", "core2")
        val router = routingDriver("core2")

        for (name in names) {
            val waited = administration.administer("CREATE DATABASE $name TOPOLOGY 1 PRIMARY WAIT")
            assertThat(waited).allSatisfy { assertThat(it["success"]).isEqualTo(true) }
            assertThat(roles("core1")[name]).isEqualTo("LEADER")
            assertThat(roles("core2")).doesNotContainKey(name)

            // Without retrying, as Cypher Shell runs a script.
            router.session(SessionConfig.forDatabase(name)).use {
                it.run("CREATE (:Film {title: 'Up'})").consume()
            }
        }
    }

    @Test
    @Order(2)
    fun `a member cut off for half a minute catches up within seconds once the network heals`() {
        val member = members.getValue("core3")
        val others = listOf("core1", "core2")
        // Caught up with what it missed in the test before.
        await withPollInterval
            Duration.ofMillis(500) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                val commits = commits(members.values)
                assertThat(applied(member)).allSatisfy { database, index ->
                    assertThat(index).isGreaterThanOrEqualTo(commits.getValue(database))
                }
            }

        faults = NetworkFaults(member).also { it.cutRaft() }
        val cutAt = System.nanoTime()
        // Entries the member misses, in both groups it hosts.
        leaderOf("system", others)
            .rows("CREATE USER missed SET PASSWORD 'missed-secret' CHANGE NOT REQUIRED", "system")
        leaderOf("neo4j", others).rows("CREATE (:Missed)")
        Thread.sleep(maxOf(0, CUT_OFF.toMillis() - (System.nanoTime() - cutAt) / 1_000_000))
        val hosted = applied(member).keys
        val missed = commits(others.map(members::getValue)).filterKeys { it in hosted }
        val healedAt = System.nanoTime()
        faults!!.healRaft()

        await withPollInterval
            Duration.ofMillis(100) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                val applied = applied(member)
                assertThat(missed).allSatisfy { database, index ->
                    assertThat(applied[database])
                        .describedAs("$database")
                        .isGreaterThanOrEqualTo(index)
                }
            }
        // TCP waits twice as long before each resend of what a peer did not acknowledge: half a
        // minute into a cut, the leader's next resend on its connection to the member is about 20
        // seconds away.
        assertThat((System.nanoTime() - healedAt) / 1_000_000)
            .describedAs("ms from healing until the member applied what it missed")
            .isLessThan(CATCH_UP_TARGET.toMillis())
    }

    private companion object {
        val CUT_OFF: Duration = Duration.ofSeconds(30)
        val CATCH_UP_TARGET: Duration = Duration.ofSeconds(10)
    }
}
