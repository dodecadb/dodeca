package org.dozendb.it

import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.lifecycle.Startables

/**
 * Disaster recovery as `OPERATOR_GUIDE.md` describes it: a lost cluster recreated from backups of
 * every database; a cluster that lost a majority of its servers recovered as Neo4j Enterprise's
 * disaster recovery guide shows, `system` first, then its servers, then its databases; a database
 * recreated from a backup, and a copy quarantined and lifted.
 */
class DisasterRecoveryIT {
    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val authority = TestAuthority("DozenDB IT backup CA")
    private val containers = mutableListOf<GenericContainer<*>>()
    private val volumes = mutableListOf<String>()

    private fun volume(name: String): String = "dozendb-it-$run-$name".also { volumes += it }

    @AfterEach
    fun stop() {
        containers.forEach {
            if (it is DozenDbContainer) {
                it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            }
            it.stop()
        }
        volumes.forEach(DozenDbContainer::removeVolume)
        network.close()
    }

    private fun cluster(
        aliases: List<String>,
        founderData: String? = null,
    ): List<DozenDbContainer> {
        val members = aliases.map { alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .backupServer(authority)
                .apply {
                    if (alias == aliases.first() && founderData != null) withDataVolume(founderData)
                }
                .also { containers += it }
        }
        Startables.deepStart(members).join()
        return members
    }

    /** Runs [cypher] on whichever of [members] leads [database], once one does. */
    private fun write(members: List<DozenDbContainer>, database: String, cypher: String) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                assertThat(members.map { it.run(cypher, database) }.count { it is Outcome.Rows })
                    .describedAs("$database written by its leader")
                    .isEqualTo(1)
            }
    }

    /**
     * Runs the administration command [cypher] on the first of [members] that takes it: commands
     * such as DEALLOCATE are taken by every member, procedures only by the leader of `system`.
     */
    private fun administer(members: List<DozenDbContainer>, cypher: String) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                val outcomes = mutableListOf<Outcome>()
                for (member in members) {
                    outcomes += member.run(cypher, "system")
                    if (outcomes.last() is Outcome.Rows) break
                }
                assertThat(outcomes).describedAs(cypher).anyMatch { it is Outcome.Rows }
            }
    }

    /** A server of a cluster with [initialMembers], its data in the volume of [alias]. */
    private fun server(alias: String, initialMembers: List<String>): DozenDbContainer =
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .clusterMember(initialMembers.map { "$it:7700" }, primaries = 3)
            .withDataVolume("dozendb-it-$run-$alias".also { if (it !in volumes) volumes += it })
            .also { containers += it }

    /** Saves the logs of [server], which is done with, apart, and removes its container. */
    private fun retire(server: DozenDbContainer) {
        server.saveLogs(
            DozenDbContainer.logsDirectory.resolve(javaClass.simpleName).resolve("retired")
        )
        server.stop()
        containers -= server
    }

    /** Runs `neo4j-admin` with [arguments] on the stopped server [alias]'s data volume. */
    private fun adminOn(alias: String, vararg arguments: String): String =
        AdminContainer(
                DozenDbContainer.dozenDbImage,
                "admin-$alias",
                network,
                mapOf("dozendb-it-$run-$alias" to "/data"),
            )
            .use {
                it.start()
                it.adminOk(*arguments).also { _ -> it.shell("chown -R 7474:7474 /data") }
            }

    /** The aliases of the servers of [members] that [database] is placed on, by server id. */
    private fun hosts(
        members: List<DozenDbContainer>,
        ids: Map<String, String>,
        database: String,
    ): Set<String> {
        val placed =
            members.first().rows("SHOW DATABASE $database YIELD serverID", "system").map {
                it["serverID"] as String
            }
        return ids.filterValues { it in placed }.keys
    }

    /** Waits until each of [members] holds [count] nodes with [label] in [database]. */
    private fun awaitCount(
        members: List<DozenDbContainer>,
        database: String,
        label: String,
        count: Long,
        timeout: Duration = Duration.ofMinutes(2),
    ) {
        await withPollInterval
            Duration.ofSeconds(1) atMost
            timeout untilAsserted
            {
                for (member in members) {
                    assertThat(member.run("MATCH (n:$label) RETURN count(n) AS c", database))
                        .describedAs("$database on ${member.alias}")
                        .isEqualTo(Outcome.Rows(listOf(mapOf("c" to count))))
                }
            }
    }

    @Test
    fun `a cluster that lost three of five servers recovers as the disaster recovery guide shows`() {
        val aliases = listOf("s1", "s2", "s3", "s4", "s5")
        val before = aliases.map { server(it, aliases) }
        Startables.deepStart(before).join()
        write(before, "system", "CREATE DATABASE films WAIT")
        write(before, "neo4j", "UNWIND range(1, 100) AS i CREATE (:Thing {i: i})")
        write(before, "films", "UNWIND range(1, 200) AS i CREATE (:Film {i: i})")
        val ids = before.associate { it.alias to it.memberId() }
        val byAlias = before.associateBy { it.alias }
        val neo4jOn = hosts(before, ids, "neo4j")
        val filmsOn = hosts(before, ids, "films")
        // Every copy holds the writes acknowledged before the disaster.
        awaitCount(neo4jOn.map(byAlias::getValue), "neo4j", "Thing", 100)
        awaitCount(filmsOn.map(byAlias::getValue), "films", "Film", 200)
        // Two survivors: one hosts both databases, the other films only, so that neo4j and system
        // lose their majorities and films keeps its.
        val survivors = listOf((neo4jOn intersect filmsOn).min(), (filmsOn - neo4jOn).min())
        val lost = aliases - survivors.toSet()
        lost.forEach { byAlias.getValue(it).kill() }

        // 1. The system database: the survivors are unbound from it and re-form it with two new
        // servers.
        for (alias in survivors) {
            byAlias.getValue(alias).shutDown()
            val unbound = adminOn(alias, "dbms", "unbind-system-db")
            assertThat(unbound).contains(ids.getValue(alias))
        }
        survivors.forEach { retire(byAlias.getValue(it)) }
        val reforming = survivors + listOf("n1", "n2")
        val after = reforming.map { server(it, reforming) }
        Startables.deepStart(after).join()
        val allIds = ids + after.filter { it.alias !in ids }.associate { it.alias to it.memberId() }
        write(
            after,
            "system",
            "CREATE USER alice SET PASSWORD 'a long password' CHANGE NOT REQUIRED",
        )
        assertThat(after.first().rows("CALL dbms.cluster.statusCheck(['films'])", "system"))
            .isNotEmpty

        // 2. The servers: the lost ones are cordoned, so that nothing is placed on them.
        for (alias in lost) {
            write(after, "system", "CALL dbms.cluster.cordonServer('${ids.getValue(alias)}')")
        }

        // 3. The databases: neo4j, which lost its majority, is recreated from the survivor's
        // store; films, which kept it, gets its third copy back.
        write(after, "system", "CALL dbms.cluster.recreateDatabase('neo4j', {seedingServers: []})")
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertThat(hosts(after, allIds, "neo4j")).hasSize(3)
            }
        awaitCount(
            hosts(after, allIds, "neo4j").map { a -> after.single { it.alias == a } },
            "neo4j",
            "Thing",
            100,
            Duration.ofMinutes(3),
        )
        // Every lost server is deallocated: films gets its copy back elsewhere.
        administer(
            after,
            "DEALLOCATE DATABASES FROM SERVERS " + lost.joinToString { "'${ids.getValue(it)}'" },
        )
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertThat(hosts(after, allIds, "films"))
                    .hasSize(3)
                    .doesNotContainAnyElementsOf(lost)
            }
        val filmsAfter = hosts(after, allIds, "films").map { a -> after.single { it.alias == a } }
        awaitCount(filmsAfter, "films", "Film", 200, Duration.ofMinutes(3))
        // The lost servers go, once deallocated.
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                val states =
                    after.first().rows("SHOW SERVERS YIELD serverId, state", "system").associate {
                        it["serverId"] to it["state"]
                    }
                for (alias in lost) {
                    assertThat(states[ids.getValue(alias)])
                        .describedAs(alias)
                        .isEqualTo("Deallocated")
                }
            }
        for (alias in lost) {
            administer(after, "DROP SERVER '${ids.getValue(alias)}'")
        }
        assertThat(
                after.first().rows("SHOW SERVERS YIELD serverId", "system").map { it["serverId"] }
            )
            .containsExactlyInAnyOrderElementsOf(reforming.map(allIds::getValue))
        write(after, "neo4j", "CREATE (:Thing {i: 0})")
        write(after, "films", "CREATE (:Film {i: 0})")
        awaitCount(
            hosts(after, allIds, "neo4j").map { a -> after.single { it.alias == a } },
            "neo4j",
            "Thing",
            101,
        )
        awaitCount(filmsAfter, "films", "Film", 201)
        for (member in after) {
            assertThat(member.rows("SHOW USERS YIELD user", "system").map { it["user"] })
                .describedAs(member.alias)
                .contains("alice")
        }

        // A lost server that comes back with its old state is refused.
        val stale = lost.first()
        retire(byAlias.getValue(stale))
        server(stale, aliases)
            .waitingFor(
                Wait.forLogMessage(".*recovered its system database without this server.*", 1)
                    .withStartupTimeout(Duration.ofMinutes(3))
            )
            .start()
    }

    @Test
    fun `a database recreated from a backup, and a copy quarantined and lifted`() {
        val backups = volume("backups")
        val aliases = listOf("q1", "q2", "q3")
        val members = aliases.map { alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .backupServer(authority)
                .withVolume(backups, "/backups")
                .also { containers += it }
        }
        Startables.deepStart(members).join()
        write(members, "system", "CREATE DATABASE films WAIT")
        write(members, "films", "UNWIND range(1, 200) AS i CREATE (:Film {i: i})")
        awaitCount(members, "films", "Film", 200)
        val client =
            AdminContainer(
                    DozenDbContainer.dozenDbImage,
                    "client",
                    network,
                    mapOf(backups to "/backups"),
                )
                .also {
                    containers += it
                    it.start()
                }
                .backupClient(authority.issue(names = listOf("client")))
        val printed =
            client.adminOk(
                "database",
                "backup",
                "--from=q2:${DozenDbContainer.BACKUP_PORT}",
                "--to-path=/backups",
                "--type=FULL",
                "films",
            )
        val backup = checkNotNull(Regex("to (\\S+\\.backup):").find(printed)).groupValues[1]
        client.shell("chmod -R a+rX /backups")
        write(members, "films", "UNWIND range(1, 50) AS i CREATE (:Film {i: -i})")
        awaitCount(members, "films", "Film", 250)
        val recreate = "CALL dbms.cluster.recreateDatabase('films', {seedURI: 'file://$backup'})"
        // Only users who may create and drop databases recreate them.
        write(
            members,
            "system",
            "CREATE USER bob SET PASSWORD 'a long password' CHANGE NOT REQUIRED",
        )
        write(members, "system", "CREATE ROLE creator")
        write(members, "system", "GRANT CREATE DATABASE ON DBMS TO creator")
        write(members, "system", "GRANT ROLE creator TO bob")
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                for (member in members) {
                    val refused = member.runAs("bob", "a long password", recreate, "system")
                    assertThat(refused)
                        .describedAs(member.alias)
                        .isInstanceOf(Outcome.Failed::class.java)
                    refused as Outcome.Failed
                    assertThat(refused.code).isEqualTo("Neo.ClientError.Security.Forbidden")
                    assertThat(refused.message)
                        .contains("needs the CREATE DATABASE and DROP DATABASE privileges")
                }
            }

        write(members, "system", recreate)

        // The backup's films, under a new id, on every server.
        awaitCount(members, "films", "Film", 200, Duration.ofMinutes(3))
        write(members, "films", "CREATE (:Film {i: 0})")
        awaitCount(members, "films", "Film", 201)

        val quarantined = members[1]
        quarantined.rows(
            "CALL dbms.quarantineDatabase('films', true, 'Inspecting its files')",
            "system",
        )
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                val row =
                    members[0]
                        .rows(
                            "SHOW DATABASE films YIELD serverID, currentStatus, statusMessage",
                            "system",
                        )
                        .single { it["serverID"] == quarantined.memberId() }
                assertThat(row["currentStatus"]).isEqualTo("quarantined")
                assertThat(row["statusMessage"] as String).endsWith("Inspecting its files")
            }
        // The others go on.
        write(members - quarantined, "films", "CREATE (:Film {i: 0})")
        awaitCount(members - quarantined, "films", "Film", 202)

        quarantined.rows("CALL dbms.quarantineDatabase('films', false)", "system")

        awaitCount(members, "films", "Film", 202)
    }

    private val data =
        mapOf(
            "neo4j" to ("Thing" to 100L),
            "films" to ("Film" to 200L),
            "books" to ("Book" to 300L),
        )

    @Test
    fun `a cluster restored from backups of every database on one server forms anew`() {
        val lost = cluster(listOf("a1", "a2", "a3"))
        for (database in listOf("films", "books")) {
            write(lost, "system", "CREATE DATABASE $database WAIT")
        }
        write(
            lost,
            "system",
            "CREATE USER alice SET PASSWORD 'a long password' CHANGE NOT REQUIRED",
        )
        for ((database, nodes) in data) {
            write(
                lost,
                database,
                "UNWIND range(1, ${nodes.second}) AS i CREATE (:${nodes.first} {i: i})",
            )
        }
        val backups = volume("backups")
        val client =
            AdminContainer(
                    DozenDbContainer.dozenDbImage,
                    "client",
                    network,
                    mapOf(backups to "/backups"),
                )
                .also {
                    containers += it
                    it.start()
                }
                .backupClient(authority.issue(names = listOf("client")))
        client.adminOk(
            "database",
            "backup",
            "--from=a2:${DozenDbContainer.BACKUP_PORT}",
            "--to-path=/backups",
            "--type=FULL",
            "*",
        )
        lost.forEach { it.shutDown() }

        val founderData = volume("founder")
        AdminContainer(
                DozenDbContainer.dozenDbImage,
                "restorer",
                network,
                mapOf(backups to "/backups", founderData to "/data"),
            )
            .also {
                containers += it
                it.start()
            }
            .apply {
                val restored = adminOk("database", "restore", "--from-path=/backups")
                for (database in data.keys + "system") {
                    assertThat(restored).contains("Restored database '$database'")
                }
                shell("chown -R 7474:7474 /data")
            }
        val recovered = cluster(listOf("b1", "b2", "b3"), founderData)

        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                for (member in recovered) {
                    for ((database, nodes) in data) {
                        assertThat(
                                member.run(
                                    "MATCH (n:${nodes.first}) RETURN count(n) AS c",
                                    database,
                                )
                            )
                            .describedAs("$database on ${member.alias}")
                            .isEqualTo(Outcome.Rows(listOf(mapOf("c" to nodes.second))))
                    }
                }
            }
        for (member in recovered) {
            assertThat(member.rows("SHOW USERS YIELD user", "system").map { it["user"] })
                .describedAs(member.alias)
                .contains("alice")
        }
        for ((database, nodes) in data) {
            write(recovered, database, "CREATE (:${nodes.first} {i: 0})")
        }
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(1) untilAsserted
            {
                for (member in recovered) {
                    for ((database, nodes) in data) {
                        assertThat(
                                member.run(
                                    "MATCH (n:${nodes.first}) RETURN count(n) AS c",
                                    database,
                                )
                            )
                            .describedAs("$database on ${member.alias}")
                            .isEqualTo(Outcome.Rows(listOf(mapOf("c" to nodes.second + 1))))
                    }
                }
            }
    }
}
