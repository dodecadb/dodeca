package org.dozendb.it

import java.time.Duration
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.concurrent.thread
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.testcontainers.containers.Network
import org.testcontainers.images.builder.Transferable
import org.testcontainers.lifecycle.Startables

/**
 * `neo4j-admin database import incremental` and `neo4j-admin database copy` in DozenDB's image, on
 * a server of a cluster, as Neo4j Enterprise's documentation has them done: the import on the
 * database's only primary, recreated so, its build stage while the database serves reads, then the
 * topology put back; the copy seeding a new database on every server.
 */
class ImportAndCopyIT {
    private val network = Network.newNetwork()
    private val containers = mutableListOf<DozenDbContainer>()

    @AfterEach
    fun stop() {
        containers.forEach {
            it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
        network.close()
    }

    private fun cluster(aliases: List<String>): List<DozenDbContainer> {
        val members = aliases.map { alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
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

    /** Runs the administration command [cypher] on the first of [members] that takes it. */
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

    /** Runs `neo4j-admin` in [server]'s container, as Neo4j's user; returns what it printed. */
    private fun admin(server: DozenDbContainer, vararg arguments: String): String {
        val result = server.execInContainer("su-exec", "neo4j:neo4j", "neo4j-admin", *arguments)
        check(result.exitCode == 0) {
            "${server.alias}: neo4j-admin ${arguments.joinToString(" ")} failed with " +
                "${result.exitCode}:\n${result.stdout}${result.stderr}"
        }
        return result.stdout
    }

    /** Puts a file at [path] in [server]'s container that Neo4j's user can read. */
    private fun file(server: DozenDbContainer, path: String, lines: List<String>) {
        server.copyFileToContainer(
            Transferable.of(lines.joinToString("\n", postfix = "\n").toByteArray(), 420),
            path,
        )
    }

    private fun count(server: DozenDbContainer, database: String, pattern: String): Long =
        server.rows("MATCH $pattern RETURN count(*) AS c", database).single()["c"] as Long

    /** Waits until each of [members] counts [expected] matches of [pattern] in [database]. */
    private fun awaitCount(
        members: List<DozenDbContainer>,
        database: String,
        pattern: String,
        expected: Long,
    ) {
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                for (member in members) {
                    assertThat(count(member, database, pattern))
                        .describedAs("$pattern in $database on ${member.alias}")
                        .isEqualTo(expected)
                }
            }
    }

    /** Waits until [database] is online on exactly the servers [members] have the ids of. */
    private fun awaitHosts(members: List<DozenDbContainer>, database: String, ids: Set<String>) {
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                val rows =
                    members
                        .first()
                        .rows(
                            "SHOW DATABASE $database YIELD serverID, currentStatus",
                            "system",
                        )
                assertThat(rows.map { it["serverID"] }.toSet()).isEqualTo(ids)
                assertThat(rows).allMatch { it["currentStatus"] == "online" }
            }
    }

    /** Films with [persons] persons in a chain and a uniqueness constraint, on three primaries. */
    private fun films(members: List<DozenDbContainer>, persons: Int) {
        administer(members, "CREATE DATABASE films TOPOLOGY 3 PRIMARIES WAIT")
        write(members, "films", "CREATE CONSTRAINT person_id FOR (p:Person) REQUIRE p.id IS UNIQUE")
        write(
            members,
            "films",
            "UNWIND range(0, ${persons - 1}) AS i CREATE (:Person {id: 'p' + i, n: i})",
        )
        write(
            members,
            "films",
            "UNWIND range(0, ${persons - 2}) AS i MATCH (a:Person {id: 'p' + i}), " +
                "(b:Person {id: 'p' + (i + 1)}) CREATE (a)-[:NEXT]->(b)",
        )
        awaitCount(members, "films", "(:Person)", persons.toLong())
    }

    @Test
    fun `an incremental import goes into a cluster's database by Neo4j Enterprise's procedure, read during its build`() {
        val members = cluster(listOf("i1", "i2", "i3"))
        films(members, PERSONS)
        val importer = members.first()
        val id = importer.memberId()
        // New persons p1000 to p1499, each next to the one before it, p999 to p1000 too.
        file(
            importer,
            "/tmp/m22/persons.csv",
            listOf("id:ID{label:Person},n:long") + (PERSONS until PERSONS + NEW).map { "p$it,$it" },
        )
        file(
            importer,
            "/tmp/m22/next.csv",
            listOf(":START_ID,:END_ID") +
                (PERSONS - 1 until PERSONS + NEW - 1).map { "p$it,p${it + 1}" },
        )
        val input =
            arrayOf(
                "--nodes=Person=/tmp/m22/persons.csv",
                "--relationships=NEXT=/tmp/m22/next.csv",
                "--report-file=/tmp/import.report",
            )
        fun stage(stage: String) =
            admin(
                importer,
                "database",
                "import",
                "incremental",
                "--force",
                "--stage=$stage",
                "films",
                *input,
            )

        // The database on the importing server only.
        administer(
            members,
            "CALL dbms.recreateDatabase('films', {seedingServers: ['$id'], primaries: 1})",
        )
        awaitHosts(members, "films", setOf(id))
        administer(members, "STOP DATABASE films WAIT")
        stage("prepare")
        administer(members, "ALTER DATABASE films SET ACCESS READ ONLY")
        administer(members, "START DATABASE films WAIT")
        val reading = AtomicBoolean(true)
        val reads = mutableListOf<Outcome>()
        val reader = thread {
            while (reading.get()) reads +=
                importer.run("MATCH (p:Person) RETURN count(p) AS c", "films")
        }
        try {
            stage("build")
        } finally {
            reading.set(false)
            reader.join()
        }
        assertThat(reads).isNotEmpty.allMatch {
            it is Outcome.Rows && it.rows.single()["c"] == PERSONS.toLong()
        }
        administer(members, "STOP DATABASE films WAIT")
        stage("merge")
        administer(members, "START DATABASE films WAIT")
        administer(members, "ALTER DATABASE films SET ACCESS READ WRITE")
        administer(members, "ALTER DATABASE films SET TOPOLOGY 3 PRIMARIES")

        val total = (PERSONS + NEW).toLong()
        awaitCount(members, "films", "(:Person)", total)
        awaitCount(members, "films", "(:Person)-[:NEXT]->(:Person)", total - 1)
        // Through the constraint's index, which the merge kept up to date.
        assertThat(
                importer.rows(
                    "MATCH (p:Person {id: 'p1499'})<-[:NEXT*500]-(q) RETURN q.id AS id",
                    "films",
                )
            )
            .containsExactly(mapOf("id" to "p999"))
        write(members, "films", "CREATE (:Person {id: 'after'})")
        awaitCount(members, "films", "(:Person)", total + 1)
        assertThat(members.first().run("CREATE (:Person {id: 'p1200'})", "films"))
            .describedAs("a duplicate of an imported key")
            .isInstanceOf(Outcome.Failed::class.java)
    }

    @Test
    fun `a copy made on a cluster's server seeds a new database on every server`() {
        val members = cluster(listOf("c1", "c2", "c3"))
        films(members, PERSONS)
        write(members, "films", "UNWIND range(1, 10) AS i CREATE (:Temp {i: i})")
        awaitCount(members, "films", "(:Temp)", 10)
        administer(members, "STOP DATABASE films WAIT")
        val seeder = members.first()
        val id = seeder.memberId()
        seeder.execInContainer("sh", "-c", "mkdir -p /tmp/m22 && chown neo4j:neo4j /tmp/m22")

        val out =
            admin(
                seeder,
                "database",
                "copy",
                "films",
                "films2",
                "--ignore-nodes-with-labels=Temp",
                "--to-path-schema=/tmp/m22",
            )

        assertThat(out)
            .contains(
                "CREATE DATABASE films2 OPTIONS {existingData: 'use', existingDataSeedServer: '$id'}"
            )
        administer(
            members,
            "CREATE DATABASE films2 TOPOLOGY 3 PRIMARIES OPTIONS {existingData: 'use', " +
                "existingDataSeedServer: '$id'} WAIT",
        )
        val schema = seeder.execInContainer("cat", "/tmp/m22/films2-schema.cypher").stdout
        val statements = schema.lines().filter { it.isNotBlank() }.map { it.removeSuffix(";") }
        assertThat(statements).hasSize(1)
        statements.forEach { write(members, "films2", it) }
        awaitCount(members, "films2", "(:Person)", PERSONS.toLong())
        awaitCount(members, "films2", "(:Person)-[:NEXT]->(:Person)", PERSONS - 1L)
        awaitCount(members, "films2", "(:Temp)", 0)
        await.ignoreExceptions() withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                for (member in members) {
                    assertThat(
                            member.rows("SHOW CONSTRAINTS YIELD name", "films2").map { it["name"] }
                        )
                        .describedAs(member.alias)
                        .containsExactly("person_id")
                }
            }
        write(members, "films2", "CREATE (:Person {id: 'after'})")
        awaitCount(members, "films2", "(:Person)", PERSONS + 1L)

        // A copy as a backup in the server's seeds directory, which seeds a database on every
        // server.
        seeder.execInContainer(
            "sh",
            "-c",
            "mkdir -p $SEEDS && chown neo4j:neo4j $SEEDS",
        )
        val backup =
            admin(
                seeder,
                "database",
                "copy",
                "films",
                "films3",
                "--copy-schema",
                "--target-format=backup",
                "--target-location=$SEEDS",
            )
        assertThat(backup)
            .contains(
                "Wrote the copy of database 'films' as a full backup of 'films3' to $SEEDS/films3-"
            )
        administer(
            members,
            "CREATE DATABASE films3 TOPOLOGY 3 PRIMARIES OPTIONS {seedURI: 'server://$id/'} WAIT",
        )
        awaitCount(members, "films3", "(:Person)", PERSONS.toLong())
        awaitCount(members, "films3", "(:Temp)", 10)
        administer(members, "START DATABASE films WAIT")
        awaitCount(members, "films", "(:Temp)", 10)
    }

    private companion object {
        /** `server.directories.seeds`, by default, in DozenDB's image. */
        const val SEEDS = "/var/lib/neo4j/seeds"
        const val PERSONS = 1000
        const val NEW = 500
    }
}
