package org.dozendb.it

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestPorts
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Config
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.net.ServerAddress
import org.neo4j.driver.summary.QueryProfile
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables

/**
 * Routing policies and page cache warmup in DozenDB's image: drivers that name a policy in their
 * URI, as Neo4j's drivers do, against servers tagged in two regions; a server restarted with the
 * warmup on, and with it off.
 */
class PoliciesAndWarmupIT {
    private val network = Network.newNetwork()
    private val containers = mutableListOf<DozenDbContainer>()
    private val drivers = mutableListOf<Driver>()
    private val volumes = mutableListOf<String>()

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        containers.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        volumes.forEach(DozenDbContainer::removeVolume)
        network.close()
    }

    /** A routing driver seeded at every one of [ports], naming [policy] if any. */
    private fun driver(ports: List<Int>, policy: String? = null): Driver =
        GraphDatabase.driver(
                "neo4j://127.0.0.1:${ports.first()}" + (policy?.let { "?policy=$it" } ?: ""),
                AuthTokens.basic("neo4j", DozenDbContainer.PASSWORD),
                Config.builder()
                    .withResolver { ports.map { ServerAddress.of("127.0.0.1", it) }.toSet() }
                    .build(),
            )
            .also { drivers += it }

    /** The Bolt addresses of the servers that ran [count] reads of [driver]. */
    private fun readers(driver: Driver, count: Int = 30): Set<String> =
        (1..count)
            .map {
                driver.session(SessionConfig.forDatabase("neo4j")).use { session ->
                    session.executeRead { tx ->
                        tx.run("MATCH (n) RETURN count(n)").consume().server().address()
                    }
                }
            }
            .toSet()

    @Test
    fun `drivers read from the servers that the policy they name chooses`() {
        val aliases = listOf("north1", "north2", "south1")
        val ports = aliases.map { TestPorts.free() }
        val members = aliases.mapIndexed { index, alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .boltOnHostPort(ports[index])
                .withEnv("NEO4J_initial_server_tags", alias.dropLast(1))
                // Every server reads, the leader too, so that the readers do not depend on it.
                .withEnv("NEO4J_dbms_routing_reads__on__writers__enabled", "true")
                .withEnv(POLICIES + "north", "tags(north)")
                .withEnv(POLICIES + "south__only", "tags(south); halt()")
                .also { containers += it }
        }
        Startables.deepStart(members).join()
        // The cluster creates neo4j once the servers started. A routing table from before every
        // server hosts it names fewer readers, and a driver keeps it for its time to live.
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                val copies =
                    members
                        .first()
                        .rows("SHOW DATABASE neo4j YIELD currentStatus, writer", "system")
                assertThat(copies.map { it["currentStatus"] })
                    .describedAs("copies of neo4j")
                    .containsExactly("online", "online", "online")
                assertThat(copies.count { it["writer"] == true }).describedAs("writers").isOne()
            }
        val address = aliases.indices.associate { aliases[it] to "127.0.0.1:${ports[it]}" }
        val plain = driver(ports)
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(readers(plain)).describedAs("no policy: all()").hasSize(3)
            }
        plain.session().use { it.executeWrite { tx -> tx.run("CREATE (:Written)").consume() } }

        assertThat(readers(driver(ports, "north")))
            .containsExactlyInAnyOrder(address["north1"], address["north2"])
        val south = driver(ports, "south_only")
        assertThat(readers(south)).containsExactly(address["south1"])
        south.session().use { session ->
            assertThat(session.executeWrite { tx -> tx.run("CREATE (:Written)").consume() })
                .describedAs("writes go to the leader whatever the policy")
                .isNotNull()
        }

        val failure = catchThrowable { readers(driver(ports, "west"), count = 1) }
        // The driver's error holds each router's refusal among its suppressed errors' causes.
        fun messages(error: Throwable): Sequence<String> =
            listOfNotNull(error.message).asSequence() +
                (listOfNotNull(error.cause) + error.suppressed).asSequence().flatMap(::messages)
        val messages = messages(failure).toList()
        assertThat(messages).describedAs("$failure").anyMatch {
            "Policy definition for 'west' could not be found." in it
        }
    }

    @Test
    fun `a restarted server loads the pages it held, unless its warmup is off`() {
        val volume = "dozendb-it-${UUID.randomUUID().toString().take(8)}-warmup"
        volumes += volume
        fun server(alias: String, vararg settings: Pair<String, String>) =
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .withDataVolume(volume)
                .apply { settings.forEach { (name, value) -> withEnv(name, value) } }
                .also {
                    containers += it
                    it.start()
                }
        val first = server("warm")
        first.run("UNWIND range(1, $PERSONS) AS i CREATE (:Person {id: i, name: 'person ' + i})")
        assertThat(misses(first)).describedAs("misses after the writes").isZero()
        first.shutDown()

        first.restart()

        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(first.debugLog())
                    .containsPattern("neo4j/\\w+] Page cache warmup loaded [1-9]\\d* pages")
            }
        assertThat(misses(first)).describedAs("misses after the warmup").isZero()
        first.shutDown()

        val cold = server("cold", "NEO4J_db_memory_pagecache_warmup_enable" to "false")
        assertThat(misses(cold)).describedAs("misses without the warmup").isPositive()
        assertThat(cold.debugLog()).doesNotContain("Page cache warmup loaded")
    }

    /** The page cache misses of reading every person on [server]. */
    private fun misses(server: DozenDbContainer): Long =
        server
            .summary("PROFILE MATCH (p:Person) RETURN sum(size(p.name)) AS size")
            .queryProfile()
            .orElseThrow()
            .misses()

    private fun QueryProfile.misses(): Long =
        pageCacheMisses().orElse(0) + children().sumOf { it.misses() }

    private companion object {
        const val POLICIES = "NEO4J_dbms_routing_load__balancing_config_server__policies_"
        const val PERSONS = 20_000
    }
}
