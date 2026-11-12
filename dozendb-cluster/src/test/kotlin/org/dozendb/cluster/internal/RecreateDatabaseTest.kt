package org.dozendb.cluster.internal

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.dozendb.backup.cli.BackupCommand
import org.dozendb.cluster.internal.ClusterHarness.Companion.AWAIT
import org.dozendb.cluster.internal.admin.CommandResult
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.security.PrivilegeRecords
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/**
 * `dbms.recreateDatabase`, as Neo4j Enterprise documents it: a database, also one that lost a
 * majority of its primaries, is recreated from a seed under a new id, keeping its name and
 * privileges.
 */
class RecreateDatabaseTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    @Test
    fun `a database recreated from a backup gets a new id, keeps its privileges, and takes writes`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.administer("CREATE ROLE viewer")
        cluster.administer("GRANT MATCH {*} ON GRAPH $FILMS TO viewer")
        val seed = cluster.backup(cluster.members[0])
        cluster.write(FILMS, 2)
        val old = cluster.members[0].databaseId(FILMS)
        val privileges = cluster.privileges("viewer")

        cluster.administer("CALL dbms.recreateDatabase('$FILMS', {seedURI: '${seed.toUri()}'})")

        cluster.awaitFilms(cluster.members, 3)
        val new = cluster.members[0].databaseId(FILMS)
        assertThat(new).isNotEqualTo(old)
        assertThat(cluster.members.map { it.databaseId(FILMS) }).containsOnly(new)
        assertThat(cluster.privileges("viewer")).isEqualTo(privileges)
        cluster.awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
            val databases =
                tx.findNodes(PrivilegeRecords.PRIVILEGE).use { nodes ->
                    nodes
                        .asSequence()
                        .mapNotNull { it.getProperty(PrivilegeRecords.DATABASE, null) }
                        .toList()
                }
            assertThat(databases).contains(new.toString()).doesNotContain(old.toString())
        }
        // The old copies and their Raft group are gone everywhere.
        await atMost
            AWAIT untilAsserted
            {
                for (member in cluster.members) {
                    assertThat(member.home.resolve("data/raft/$old"))
                        .describedAs("$member")
                        .doesNotExist()
                }
            }
        cluster.write(FILMS, 4)
        cluster.awaitFilms(cluster.members, 7)
    }

    @Test
    fun `a database is recreated from the seeds directory of the server a server URI names`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        val seed = cluster.backup(cluster.members[0])
        cluster.write(FILMS, 2)
        val reader = cluster.members[2]
        Files.copy(seed, Files.createDirectories(reader.home.resolve("seeds")).resolve("f.backup"))
        cluster.administer("RENAME SERVER '${reader.serverId}' TO 'gamma'")

        assertThat(
                catchThrowable {
                    cluster.administer(
                        "CALL dbms.recreateDatabase('$FILMS', {seedURI: 'server://delta/f.backup'})"
                    )
                }
            )
            .hasMessageContaining(
                "Could not recreate database '$FILMS': there is no such server. SHOW SERVERS " +
                    "lists the servers."
            )
        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedURI: 'server://gamma/f.backup'})"
        )

        cluster.awaitFilms(cluster.members, 3)
    }

    @Test
    fun `the default database is recreated from a backup as any other`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        val leader = cluster.awaitLeader()
        leader.database().executeTransactionally("UNWIND range(1, 3) AS i CREATE (:Film {i: i})")
        val seed = cluster.backup(leader, DEFAULT)
        leader.database().executeTransactionally("CREATE (:Film)")
        val old = cluster.members[0].databaseId(DEFAULT)

        cluster.administer("CALL dbms.recreateDatabase('$DEFAULT', {seedURI: '${seed.toUri()}'})")

        cluster.awaitFilms(cluster.members, 3, DEFAULT)
        assertThat(cluster.members.map { it.databaseId(DEFAULT) }).doesNotContain(old).hasSize(3)
        cluster.awaitLeader().database().executeTransactionally("CREATE (:Film)")
        cluster.awaitFilms(cluster.members, 4, DEFAULT)
    }

    @Test
    fun `a database that lost a majority of its primaries is recreated from a backup on the servers left`() {
        val cluster =
            ClusterHarness(root, 5, settings = mapOf(ClusterSettings.defaultPrimaries to 3)).also {
                harness = it
            }
        // films goes to servers 0 to 2.
        cluster.members.drop(3).forEach {
            it.settings = mapOf(DozenDbSettings.initialServerDeniedDatabases to listOf(FILMS))
        }
        cluster.start()
        val hosts = cluster.members.take(3)
        cluster.write(FILMS, 3, hosts)
        val seed = cluster.backup(hosts[2])
        val lost = hosts.take(2)
        val lostIds = lost.map { it.serverId }
        lost.forEach { it.stop() }
        val left = cluster.members.drop(2)
        for (id in lostIds) {
            cluster.administer("CALL dbms.cluster.cordonServer('$id')", among = left)
        }
        // The servers that may host it now.
        cluster.members.drop(3).forEach {
            cluster.administer(
                "ALTER SERVER '${it.serverId}' SET OPTIONS {deniedDatabases: []}",
                among = left,
            )
        }

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedURI: '${seed.toUri()}'})",
            among = left,
        )

        cluster.awaitFilms(left, 3)
        cluster.write(FILMS, 2, left)
        cluster.awaitFilms(left, 5)
    }

    @Test
    fun `a database that lost a majority of its primaries is recreated from the store of the one left`() {
        val cluster =
            ClusterHarness(root, 5, settings = mapOf(ClusterSettings.defaultPrimaries to 3)).also {
                harness = it
            }
        cluster.members.drop(3).forEach {
            it.settings = mapOf(DozenDbSettings.initialServerDeniedDatabases to listOf(FILMS))
        }
        cluster.start()
        val hosts = cluster.members.take(3)
        cluster.write(FILMS, 3, hosts)
        cluster.awaitFilms(hosts, 3)
        val survivor = hosts[2]
        val lostIds = hosts.take(2).map { it.serverId }
        hosts.take(2).forEach { it.stop() }
        val left = cluster.members.drop(2)
        for (id in lostIds) {
            cluster.administer("CALL dbms.cluster.cordonServer('$id')", among = left)
        }
        cluster.members.drop(3).forEach {
            cluster.administer(
                "ALTER SERVER '${it.serverId}' SET OPTIONS {deniedDatabases: []}",
                among = left,
            )
        }
        val old = survivor.databaseId(FILMS)

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedingServers: ['${survivor.serverId}']})",
            among = left,
        )

        // Its store, as it was, on every server left, under a new id.
        cluster.awaitFilms(left, 3)
        assertThat(left.map { it.databaseId(FILMS) }).doesNotContain(old)
        cluster.write(FILMS, 2, left)
        cluster.awaitFilms(left, 5)
        left.forEach { it.stop() }
        for (member in left) {
            assertThat(member.checkConsistency(FILMS).isSuccessful)
                .describedAs("$member consistent")
                .isTrue
        }
    }

    @Test
    fun `a database is recreated from the stores of every server hosting it, refused while one is quarantined`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.awaitFilms(cluster.members, 3)
        val old = cluster.members[0].databaseId(FILMS)
        val quarantined = cluster.members[1]
        quarantined
            .database()
            .executeTransactionally(
                "CYPHER 5 CALL dbms.quarantineDatabase('$FILMS', true, 'testing')"
            )

        assertThat(refusal(cluster, "{seedingServers: []}"))
            .contains("its copies on ${quarantined.serverId} are in quarantine")
        assertThat(refusal(cluster, "{seedingServers: [], primaries: 2}"))
            .contains("it would have 2 copies, fewer than its 3 seeding servers")

        quarantined
            .database()
            .executeTransactionally(
                "CALL dbms.unquarantineDatabase('${quarantined.serverId}', '$FILMS')"
            )
        cluster.awaitFilms(cluster.members, 3)
        cluster.administer("CALL dbms.recreateDatabase('$FILMS', {seedingServers: []})")

        cluster.awaitFilms(cluster.members, 3)
        assertThat(cluster.members.map { it.databaseId(FILMS) }).doesNotContain(old)
        assertThat(cluster.members.map { it.databaseId(FILMS) }.distinct()).hasSize(1)
        cluster.write(FILMS, 2)
        cluster.awaitFilms(cluster.members, 5)
        assertThat(cluster.shown(FILMS).values.map { it["statusMessage"] }).containsOnly("")
    }

    @Test
    fun `a seeder that was down and is behind waits to be reported, then the latest store creates it`() {
        val cluster =
            ClusterHarness(root, 3, settings = ClusterHarness.METRICS).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.awaitFilms(cluster.members, 3)
        val behind = cluster.members[0]
        val behindId = behind.serverId
        val seeders = cluster.members.map { it.serverId }
        behind.stop()
        val up = cluster.members.drop(1)
        cluster.write(FILMS, 2, up)
        cluster.awaitFilms(up, 5)

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedingServers: " +
                "${seeders.joinToString(prefix = "[", postfix = "]") { "'$it'" }}})",
            among = up,
        )

        await atMost
            AWAIT untilAsserted
            {
                assertThat(cluster.shown(FILMS, up).values.map { it["statusMessage"] })
                    .contains("Waiting for seeder $behindId to report its store.")
            }
        behind.start()
        // The store of the seeders up to date, not the one behind.
        cluster.awaitFilms(cluster.members, 5)
        assertThat(cluster.members.map { it.databaseId(FILMS) }.distinct()).hasSize(1)
        // The seeders pulled what they lacked, the one behind too, then turned primary: no copy of
        // the new database.
        await atMost
            AWAIT untilAsserted
            {
                assertThat(cluster.shown(FILMS).values.map { it["role"] to it["currentStatus"] })
                    .containsOnly("primary" to "online")
            }
        assertThat(cluster.members.map { it.copiesReceived() }).containsOnly(0.0)
        cluster.write(FILMS, 1)
        cluster.awaitFilms(cluster.members, 6)
    }

    @Test
    fun `seeders with stores of different databases fail the recreation, shown, until it is recreated from good ones`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.write(SERIES, 1)
        cluster.awaitFilms(cluster.members, 3)
        cluster.awaitFilms(cluster.members, 1, SERIES)
        val wrong = cluster.members[2]
        val wrongId = wrong.serverId
        val wrongName = wrong.serverName()
        val good = cluster.members.take(2)
        val goodIds = good.map { it.serverId }
        val old = wrong.databaseId(FILMS)
        wrong.stop()
        // A store of another database in place of its copy, which no Raft group counts.
        for (kind in listOf("databases", "transactions")) {
            val data = wrong.home.resolve("data/$kind")
            data.resolve(FILMS).toFile().deleteRecursively()
            data.resolve(SERIES).toFile().copyRecursively(data.resolve(FILMS).toFile())
        }
        wrong.home.resolve("data/raft/$old").toFile().deleteRecursively()

        assertThat(refusal(cluster, "{seedingServers: []}", among = good))
            .contains("server $wrongName hosts a copy of it but is not available")
            .contains("cordon it")
        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedingServers: " +
                "${(goodIds + wrongId).joinToString(prefix = "[", postfix = "]") { "'$it'" }}})",
            among = good,
        )
        wrong.start()

        // The election takes seeders by id.
        val (first, second) =
            listOf(goodIds.minOf { UUID.fromString(it) }, UUID.fromString(wrongId)).sorted()
        await atMost
            AWAIT untilAsserted
            {
                assertThat(cluster.shown(FILMS).values.map { it["statusMessage"] })
                    .contains(
                        "Seeders $first and $second have incompatible storeIds. All seeders " +
                            "must have compatible storeIds."
                    )
            }

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {seedingServers: " +
                "${goodIds.joinToString(prefix = "[", postfix = "]") { "'$it'" }}})"
        )

        cluster.awaitFilms(cluster.members, 3)
        cluster.write(FILMS, 2)
        cluster.awaitFilms(cluster.members, 5)
        assertThat(cluster.shown(FILMS).values.map { it["statusMessage"] }).containsOnly("")
    }

    @Test
    fun `clearData recreates a database empty, in the store format asked for, keeping privileges`() {
        val cluster = ClusterHarness(root, 3).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 3)
        cluster.administer("CREATE ROLE viewer")
        cluster.administer("GRANT MATCH {*} ON GRAPH $FILMS TO viewer")
        cluster
            .awaitLeader(FILMS)
            .database(FILMS)
            .executeTransactionally("CREATE INDEX film_i FOR (f:Film) ON (f.i)")
        val old = cluster.members[0].databaseId(FILMS)
        val privileges = cluster.privileges("viewer")

        cluster.administer(
            "CALL dbms.recreateDatabase('$FILMS', {clearData: true, storeFormat: 'standard'})"
        )

        cluster.awaitFilms(cluster.members, 0)
        assertThat(cluster.members.map { it.databaseId(FILMS) }).doesNotContain(old)
        assertThat(cluster.privileges("viewer")).isEqualTo(privileges)
        for (member in cluster.members) {
            assertThat(member.indexes(FILMS)).describedAs("$member").doesNotContain("film_i")
        }
        assertThat(cluster.stores(FILMS)).isNotEmpty.allMatch { "standard" in it }
        // Without a format, the one the database was created in, here the one it has, which
        // Enterprise keeps; a copy in quarantine goes with the old database.
        cluster.write(FILMS, 2)
        cluster.awaitFilms(cluster.members, 2)
        cluster.members[1]
            .database()
            .executeTransactionally(
                "CYPHER 5 CALL dbms.quarantineDatabase('$FILMS', true, 'testing')"
            )
        cluster.administer("CALL dbms.recreateDatabase('$FILMS', {clearData: true})")
        cluster.awaitFilms(cluster.members, 0)
        assertThat(cluster.stores(FILMS)).isNotEmpty.allMatch { "standard" in it }
        assertThat(cluster.shown(FILMS).values.map { it["statusMessage"] }).containsOnly("")
        cluster.write(FILMS, 4)
        cluster.awaitFilms(cluster.members, 4)
    }

    @Test
    fun `recreating refuses unknown databases and options, and other options than Enterprise's`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 1)
        cluster.administer("CREATE ALIAS movies FOR DATABASE $FILMS")
        val seed = "file:///nowhere/films.backup"

        fun refusal(database: String, options: String): String? =
            refusal(cluster, options, database)

        assertThat(refusal(FILMS, "{}")).contains("give seedingServers").contains("clearData")
        assertThat(refusal(FILMS, "{clearData: 'yes'}")).contains("clearData must be a boolean")
        assertThat(refusal(FILMS, "{clearData: true, seedURI: '$seed'}"))
            .contains("clearData recreates it empty, without seedURI or seedingServers")
        assertThat(refusal(FILMS, "{clearData: true, seedingServers: []}"))
            .contains("clearData recreates it empty, without seedURI or seedingServers")
        assertThat(refusal(FILMS, "{seedingServers: [], storeFormat: 'standard'}"))
            .contains("storeFormat needs clearData")
        for (format in listOf("papyrus", "block", "high_limit")) {
            assertThat(refusal(FILMS, "{clearData: true, storeFormat: '$format'}"))
                .contains("'$format' is no store format")
                .contains("aligned, standard")
        }
        assertThat(refusal(FILMS, "{seedURI: '$seed', colour: 'red'}"))
            .contains("unknown option 'colour'")
        assertThat(refusal(FILMS, "{seedURI: '$seed', seedingServers: ['a']}"))
            .contains("seedingServers must be empty")
        assertThat(refusal(FILMS, "{seedURI: '$seed', secondaries: 1}")).contains("needs primaries")
        assertThat(refusal(FILMS, "{seedURI: '$seed', primaries: 12}")).contains("not 1 to 11")
        assertThat(refusal(FILMS, "{seedingServers: ['abc']}")).contains("is not a server id")
        assertThat(refusal(FILMS, "{seedingServers: ['${UUID.randomUUID()}']}"))
            .contains("is not a server")
        assertThat(refusal("system", "{seedURI: '$seed'}")).contains("unbind-system-db")
        assertThat(refusal("movies", "{seedURI: '$seed'}")).contains("it is an alias")
        assertThat(refusal("series", "{seedURI: '$seed'}")).contains("it does not exist")
        assertThat(cluster.members[0].count(FILMS)).isEqualTo(1)
    }

    @Test
    fun `dbms_cluster_recreateDatabase recreates too, deprecated in favor of dbms_recreateDatabase`() {
        val cluster = ClusterHarness(root, 1).also { harness = it }
        cluster.start()
        cluster.write(FILMS, 1)

        val warnings =
            cluster.awaitLeader(SYSTEM).database(SYSTEM).beginTx().use { tx ->
                tx.execute("CALL dbms.cluster.recreateDatabase('$FILMS', {clearData: true})")
                    .use { result ->
                        result.resultAsString()
                        result.gqlStatusObjects.map { it.statusDescription() }
                    }
                    .also { tx.commit() }
            }

        assertThat(warnings).anySatisfy { assertThat(it).contains("dbms.recreateDatabase") }
        cluster.awaitFilms(cluster.members, 0)
    }

    /** Why recreating [database] with [options] is refused, with its causes; null if it is not. */
    private fun refusal(
        cluster: ClusterHarness,
        options: String,
        database: String = FILMS,
        among: List<ClusterHarness.Member> = cluster.members,
    ): String? = catchThrowable {
        cluster.administer("CALL dbms.recreateDatabase('$database', $options)", among)
    }
        ?.let {
            generateSequence(it) { e -> e.cause }.joinToString(" | ") { e -> e.message.orEmpty() }
        }

    /** The rows of SHOW DATABASE [database] on the system leader among [among], by server id. */
    private fun ClusterHarness.shown(
        database: String,
        among: List<ClusterHarness.Member> = members,
    ): Map<String, Map<String, Any?>> =
        awaitLeader(SYSTEM) { among }
            .database(SYSTEM)
            .executeTransactionally(
                "SHOW DATABASE $database YIELD serverID, role, currentStatus, statusMessage",
                emptyMap(),
            ) {
                it.asSequence().associate { row -> row["serverID"] as String to row.toMap() }
            }

    /** How many copies of [FILMS] this member received since it started. */
    private fun ClusterHarness.Member.copiesReceived(): Double =
        samples()["neo4j_database_${FILMS}_dozendb_cluster_store_copy_received_total"] ?: 0.0

    /** The name of this member's server, as SHOW SERVERS has it. */
    private fun ClusterHarness.Member.serverName(): String =
        database(SYSTEM).executeTransactionally(
            "SHOW SERVERS YIELD serverId, name WHERE serverId = \$id RETURN name",
            mapOf("id" to serverId),
        ) {
            it.next()["name"] as String
        }

    /** Creates [FILMS] on [among] if needed and writes [count] films, as its leader. */
    private fun ClusterHarness.write(
        database: String,
        count: Int,
        among: List<ClusterHarness.Member> = members,
    ) {
        administer("CREATE DATABASE $database IF NOT EXISTS WAIT")
        awaitLeader(database) { among }
            .database(database)
            .executeTransactionally("UNWIND range(1, $count) AS i CREATE (:Film {i: i})")
    }

    /** Waits until each of [members] holds [count] films in [database]. */
    private fun ClusterHarness.awaitFilms(
        members: List<ClusterHarness.Member>,
        count: Long,
        database: String = FILMS,
    ) {
        await atMost
            AWAIT untilAsserted
            {
                for (member in members) {
                    assertThat(runCatching { member.count(database) }.getOrNull())
                        .describedAs("$member")
                        .isEqualTo(count)
                }
            }
    }

    private fun ClusterHarness.Member.count(database: String): Long =
        database(database).executeTransactionally(
            "MATCH (n:Film) RETURN count(n) AS count",
            emptyMap(),
        ) {
            it.next()["count"] as Long
        }

    /** The names of the indexes of [database] on this member. */
    private fun ClusterHarness.Member.indexes(database: String): List<String> =
        database(database).executeTransactionally("SHOW INDEXES YIELD name", emptyMap()) {
            it.asSequence().map { row -> row["name"] as String }.toList()
        }

    /** The store formats of the copies of [database], as SHOW DATABASES on each member tells. */
    private fun ClusterHarness.stores(database: String): List<String> = members.flatMap { member ->
        member.database(SYSTEM).executeTransactionally(
            "SHOW DATABASE $database YIELD store",
            emptyMap(),
        ) {
            it.asSequence().mapNotNull { row -> row["store"] as String? }.toList()
        }
    }

    private fun ClusterHarness.Member.databaseId(name: String): UUID =
        database(name).databaseId().databaseId().uuid()

    private fun ClusterHarness.privileges(role: String): List<Map<String, Any?>> =
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(
            "SHOW ROLE $role PRIVILEGES YIELD *",
            emptyMap(),
        ) {
            it.asSequence().map { row -> row.toMap() }.toList()
        }

    private fun ClusterHarness.administer(
        command: String,
        among: List<ClusterHarness.Member> = members,
    ) {
        awaitLeader(SYSTEM) { among }.database(SYSTEM).executeTransactionally(command)
    }

    /** A full backup of [database] from [member]. */
    private fun ClusterHarness.backup(
        member: ClusterHarness.Member,
        database: String = FILMS,
    ): Path {
        val backups = Files.createDirectories(root.resolve("backups"))
        val printed =
            admin(
                Files.createDirectories(root.resolve("client")),
                "--from=127.0.0.1:${member.backupPort()}",
                database,
                "--to-path=$backups",
                "--type=FULL",
            )
        return Path.of(checkNotNull(Regex("to (\\S+\\.backup):").find(printed.out)).groupValues[1])
    }

    private fun admin(home: Path, vararg arguments: String): CommandResult {
        val out = java.io.ByteArrayOutputStream()
        val err = java.io.ByteArrayOutputStream()
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                java.io.PrintStream(out, true),
                java.io.PrintStream(err, true),
                java.io.InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode = CommandLine(BackupCommand(context)).execute(*arguments)
        val result = CommandResult(exitCode, out.toString(), err.toString())
        assertThat(exitCode).describedAs("$result").isZero
        return result
    }

    private companion object {
        const val SYSTEM = "system"
        const val FILMS = "films"
        const val SERIES = "series"
        const val DEFAULT = ClusterHarness.DEFAULT_DATABASE
    }
}
