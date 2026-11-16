package dev.dodeca.it

import com.github.dockerjava.api.DockerClient
import java.time.Duration
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.MethodOrderer
import org.junit.jupiter.api.Order
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestMethodOrder
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.startupcheck.StartupCheckStrategy
import org.testcontainers.containers.startupcheck.StartupCheckStrategy.StartupStatus
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

/**
 * With clustering disabled, Dodeca runs as Neo4j Community does, with many databases and property
 * existence constraints. Most tests run the same statements on Dodeca and on the official Community
 * image and compare rows or errors; the tests at the end show what Dodeca adds, which Community
 * refuses.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation::class)
class StandaloneIT {

    private val community = DodecaContainer(DodecaContainer.communityImage, "community").withHttp()
    private val dodeca = DodecaContainer(DodecaContainer.dodecaImage, "dodeca").withHttp()
    private val servers = listOf(community, dodeca)

    @BeforeAll
    fun start() {
        Startables.deepStart(servers).join()
    }

    @AfterAll
    fun stop() {
        servers.forEach {
            it.saveLogs(DodecaContainer.logsDirectory.resolve(javaClass.simpleName))
            it.stop()
        }
    }

    private fun assertSame(cypher: String, database: String = "neo4j"): Outcome {
        val expected = community.run(cypher, database)
        assertThat(dodeca.run(cypher, database)).describedAs(cypher).isEqualTo(expected)
        return expected
    }

    private fun names(server: DodecaContainer, cypher: String): Set<String> =
        server.rows(cypher, "system").map { it.getValue("name") as String }.toSet()

    private fun refusedByCommunity(cypher: String, database: String = "neo4j") {
        assertThat(community.run(cypher, database))
            .describedAs("$cypher on Community")
            .isInstanceOf(Outcome.Failed::class.java)
    }

    private fun failed(cypher: String, database: String = "neo4j"): Outcome.Failed {
        val outcome = dodeca.run(cypher, database)
        assertThat(outcome).describedAs(cypher).isInstanceOf(Outcome.Failed::class.java)
        return outcome as Outcome.Failed
    }

    private fun statuses(): Map<String, String> =
        dodeca.rows("SHOW DATABASES YIELD name, currentStatus", "system").associate {
            it["name"] as String to it["currentStatus"] as String
        }

    @Test
    fun `answers the cluster status endpoints only as a cluster, of one member if need be`() {
        for (server in servers) {
            assertThat(server.get("/db/neo4j/cluster/available").statusCode())
                .describedAs(server.alias)
                .isEqualTo(404)
        }
        DodecaContainer(DodecaContainer.dodecaImage, "solo")
            .clusterMember(listOf("solo:7700"), authority = null)
            .withHttp()
            .use { solo ->
                solo.start()
                await atMost
                    Duration.ofSeconds(30) untilAsserted
                    {
                        assertThat(
                                listOf("writable", "read-only", "available").associateWith {
                                    solo.get("/db/neo4j/cluster/$it").let { r ->
                                        r.statusCode() to r.body()
                                    }
                                }
                            )
                            .isEqualTo(
                                mapOf(
                                    "writable" to (200 to "true"),
                                    "read-only" to (404 to "false"),
                                    "available" to (200 to "true"),
                                )
                            )
                    }
            }
    }

    @Test
    fun `ends the server once a database panics, by default as in Enterprise since 2025_01`() {
        DodecaContainer(DodecaContainer.dodecaImage, "panicking")
            // Transaction logs on a small disk, which a large write fills: the append fails.
            .withTmpFs(mapOf("/data/transactions" to "rw,size=8m,mode=1777"))
            .withEnv("NEO4J_db_tx__log_preallocate", "false")
            .withEnv("NEO4J_db_tx__log_rotation_size", "1M")
            .use { server ->
                server.start()

                assertThat(
                        server.run(
                            "UNWIND range(1, 20000) AS i CREATE (:Big {text: i + '$PADDING'})"
                        )
                    )
                    .isInstanceOf(Outcome.Failed::class.java)

                val docker = DockerClientFactory.instance().client()
                await atMost
                    Duration.ofMinutes(2) untilAsserted
                    {
                        assertThat(
                                docker.inspectContainerCmd(server.containerId).exec().state.running
                            )
                            .isFalse()
                    }
                assertThat(docker.inspectContainerCmd(server.containerId).exec().state.exitCodeLong)
                    .isEqualTo(1L)
                assertThat(server.logs)
                    .contains(
                        "Database neo4j panicked: shutting the server down, as " +
                            "server.panic.shutdown_on_panic is true."
                    )
            }
    }

    @Test
    fun `runs the Dodeca entry point with clustering disabled`() {
        assertThat(dodeca.logs).containsPattern("Dodeca \\S+: clustering is disabled")
        assertThat(community.logs).doesNotContain("Dodeca")
    }

    /** As DozerDB did, so that tools offer many databases. */
    @Test
    fun `reports the Enterprise edition`() {
        assertSame("CALL dbms.components() YIELD name, versions")
        // Neo4j 2026 adds a row of the Cypher versions, without an edition.
        val edition =
            "CALL dbms.components() YIELD name, edition WHERE name = 'Neo4j Kernel' " +
                "RETURN edition"
        assertThat(community.rows(edition)).containsExactly(mapOf("edition" to "community"))
        assertThat(dodeca.rows(edition)).containsExactly(mapOf("edition" to "enterprise"))
    }

    /** Neo4j would announce this server on the local network as Neo4j Enterprise. */
    @Test
    fun `announces itself on the local network only when asked`() {
        val setting =
            "SHOW SETTINGS YIELD name, value WHERE name = 'server.fleet_discovery.enabled' " +
                "RETURN value"
        assertThat(community.rows(setting, "system")).containsExactly(mapOf("value" to "true"))
        assertThat(dodeca.rows(setting, "system")).containsExactly(mapOf("value" to "false"))
    }

    /** Neo4j would report this server to udc.neo4j.com as Neo4j Enterprise. */
    @Test
    fun `sends Neo4j usage data only when asked`() {
        val setting =
            "SHOW SETTINGS YIELD name, value WHERE name = 'dbms.usage_report.enabled' RETURN value"
        assertThat(community.rows(setting, "system")).containsExactly(mapOf("value" to "true"))
        assertThat(dodeca.rows(setting, "system")).containsExactly(mapOf("value" to "false"))
        assertThat(community.logs).contains("Anonymous Usage Data is being sent to Neo4j")
        assertThat(dodeca.logs).doesNotContain("Anonymous Usage Data")
    }

    /**
     * As when a reply gets lost on its way to the test, which OrbStack's port forwarding does at
     * times.
     */
    @Test
    fun `a request that gets no reply fails within seconds`() {
        dodeca.rows("RETURN 1 AS one")
        val docker = DockerClientFactory.instance().client()
        // The kernel takes the request in; the frozen server never answers it.
        docker.pauseContainerCmd(dodeca.containerId).exec()
        try {
            val reply = CompletableFuture.supplyAsync { dodeca.run("RETURN 1 AS one") }
            assertThat(reply.get(30, TimeUnit.SECONDS)).isInstanceOf(Outcome.Failed::class.java)
        } finally {
            docker.unpauseContainerCmd(dodeca.containerId).exec()
        }
        assertThat(dodeca.rows("RETURN 1 AS one")).containsExactly(mapOf("one" to 1L))
    }

    @Test
    fun `offers Community's procedures and functions, and Enterprise's that Dodeca adds`() {
        val communityProcedures = names(community, "SHOW PROCEDURES YIELD name")
        val dodecaProcedures = names(dodeca, "SHOW PROCEDURES YIELD name")
        assertThat(communityProcedures - dodecaProcedures).isEmpty()
        assertThat(dodecaProcedures - communityProcedures)
            .containsExactlyInAnyOrder(
                "dbms.setConfigValue",
                "dbms.checkConfigValue",
                "db.checkpoint",
                "dbms.scheduler.jobs",
                "dbms.scheduler.groups",
                "dbms.scheduler.failedJobs",
                "dbms.listPools",
                "db.listLocks",
                "dbms.listActiveLocks",
                "dbms.setDefaultDatabase",
                // Under Cypher 25, the default, as Enterprise's dbms.quarantineDatabase is not.
                "dbms.unquarantineDatabase",
                "dbms.security.reloadTLS",
            )
        assertThat(names(dodeca, "SHOW FUNCTIONS YIELD name"))
            .isEqualTo(names(community, "SHOW FUNCTIONS YIELD name"))
    }

    @Test
    fun `declares only the Dodeca settings on top of Community's`() {
        val communitySettings = names(community, "SHOW SETTINGS YIELD name")
        val dodecaSettings = names(dodeca, "SHOW SETTINGS YIELD name")

        assertThat(communitySettings - dodecaSettings).isEmpty()
        assertThat(dodecaSettings - communitySettings)
            .containsExactlyInAnyOrder(
                "dbms.max_databases",
                "server.dynamic.setting.allowlist",
                "dbms.databases.seed_from_uri_providers",
                "dbms.integrations.cloud_storage.azb.blob_endpoint_suffix",
                "dbms.integrations.cloud_storage.azb.authority_endpoint",
                "server.backup.enabled",
                "server.backup.listen_address",
                "server.backup.advertised_address",
                "dbms.dodeca.cluster.enabled",
                "dbms.dodeca.cluster.initial_members",
                "dbms.dodeca.cluster.election_timeout_min",
                "dbms.dodeca.cluster.election_timeout_max",
                "dbms.cluster.raft.leader_transfer.balancing_strategy",
                "db.cluster.raft.leader_transfer.priority_tag",
                "initial.dbms.automatically_enable_free_servers",
                "initial.dbms.default_primaries_count",
                "initial.dbms.default_secondaries_count",
                "initial.server.tags",
                "initial.server.mode_constraint",
                "initial.server.allowed_databases",
                "initial.server.denied_databases",
                "dbms.routing.reads_on_writers_enabled",
                "dbms.dodeca.routing.forward_bolt_writes",
                "dbms.routing.reads_on_primaries_enabled",
                "dbms.routing.load_balancing.plugin",
                "dbms.routing.load_balancing.shuffle_enabled",
                "dbms.security.cluster_status_auth_enabled",
                "dbms.dodeca.reads.default_consistency",
                "server.dodeca.cluster.listen_address",
                "server.dodeca.cluster.advertised_address",
                "server.dodeca.cluster.raft_directory",
                "server.dodeca.cluster.single_fsync",
                "server.cluster.system_database_mode",
                "server.cluster.catchup.upstream_strategy",
                "server.cluster.catchup.connect_randomly_to_server_tags",
                "server.cluster.catchup.user_defined_upstream_strategy",
                "db.cluster.catchup.pull_interval",
                "server.routing.advertised_address",
                "server.metrics.enabled",
                "server.metrics.prefix",
                "server.metrics.filter",
                "server.metrics.deny_filter",
                "server.metrics.csv.enabled",
                "server.metrics.csv.interval",
                "server.metrics.csv.rotation.size",
                "server.metrics.csv.rotation.keep_number",
                "server.metrics.csv.rotation.compression",
                "server.directories.metrics",
                "server.metrics.graphite.enabled",
                "server.metrics.graphite.server",
                "server.metrics.graphite.interval",
                "server.metrics.jmx.enabled",
                "server.metrics.prometheus.enabled",
                "server.metrics.prometheus.endpoint",
                "dbms.security.log_successful_authentication",
                "dbms.security.keystore.path",
                "dbms.security.keystore.password",
                "dbms.security.key.name",
                "dbms.security.tls_reload_enabled",
                "dbms.security.abac.authorization_providers",
                "server.panic.shutdown_on_panic",
            )
    }

    @Test
    fun `Community refuses to start with the metrics settings Dodeca declares`() {
        val (exitCode, logs) =
            exit(DodecaContainer.communityImage, "NEO4J_server_metrics_prometheus_enabled", "true")

        assertThat(exitCode).isNotZero()
        assertThat(logs).contains("Unrecognized setting", "server.metrics.prometheus.enabled")
    }

    @Test
    fun `refuses to start with the server groups Enterprise removed, naming the setting replacing them`() {
        val (exitCode, logs) = exit(DodecaContainer.dodecaImage, "NEO4J_server_groups", "eu")

        assertThat(exitCode).isNotZero()
        assertThat(logs)
            .contains(
                "Error: Unrecognized setting. No declared setting with name: server.groups " +
                    "(removed in Neo4j 2025.01; use initial.server.tags instead)."
            )
    }

    /** The exit code and logs of a server of [image] started with the setting [env] at [value]. */
    private fun exit(image: DockerImageName, env: String, value: String): Pair<Long?, String> =
        GenericContainer(image)
            .withEnv("NEO4J_AUTH", "none")
            .withEnv(env, value)
            .withStartupCheckStrategy(Exited().withTimeout(Duration.ofMinutes(2)))
            .use {
                it.start()
                it.currentContainerInfo.state.exitCodeLong to it.logs
            }

    /** Startup completes once the container stopped, whatever its exit code. */
    private class Exited : StartupCheckStrategy() {
        override fun checkStartupState(client: DockerClient, containerId: String): StartupStatus =
            if (getCurrentState(client, containerId).running == true) StartupStatus.NOT_YET_KNOWN
            else StartupStatus.SUCCESSFUL
    }

    @Test
    fun `answers administration queries the same way`() {
        listOf(
                "SHOW DATABASES YIELD name, type, aliases, access, role, writer, requestedStatus, " +
                    "currentStatus, statusMessage, default, home, constituents RETURN * ORDER BY name",
                "SHOW DEFAULT DATABASE YIELD name, currentStatus",
                // Dodeca also shows the roles, status and home database, where Community shows
                // null, and answers SHOW ROLES, SHOW PRIVILEGES, SHOW ALIASES and SHOW SERVERS,
                // which Community refuses.
                "SHOW USERS YIELD user, passwordChangeRequired",
                "SHOW CURRENT USER YIELD user, passwordChangeRequired",
            )
            .forEach { assertSame(it, "system") }
    }

    @Test
    fun `reads, writes and enforces uniqueness the same way`() {
        listOf(
                "CREATE CONSTRAINT person_name FOR (p:Person) REQUIRE p.name IS UNIQUE",
                "CREATE INDEX person_age FOR (p:Person) ON (p.age)",
                "CREATE (:Person {name: 'ada', born: 1815})-[:KNOWS {since: 1833}]->" +
                    "(:Person {name: 'charles', born: 1791})",
                "CREATE (:Person {name: 'ada', born: 1815})",
                "MATCH (p:Person) RETURN p.name AS name, p.born AS born ORDER BY name",
                "MATCH (:Person)-[k:KNOWS]->(:Person) RETURN count(k) AS knows, max(k.since) AS since",
                "SHOW CONSTRAINTS YIELD name, type, entityType, labelsOrTypes, properties " +
                    "RETURN * ORDER BY name",
                "SHOW INDEXES YIELD name, type, entityType, labelsOrTypes, properties " +
                    "WHERE type <> 'LOOKUP' RETURN * ORDER BY name",
            )
            .forEach { assertSame(it) }
    }

    @Test
    fun `manages users the same way`() {
        listOf(
                "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED",
                "CREATE USER alice SET PASSWORD 'alice-secret-1'",
                "ALTER USER alice SET PASSWORD CHANGE REQUIRED",
                "SHOW USERS YIELD user, passwordChangeRequired ORDER BY user",
                "DROP USER alice",
                "SHOW USERS YIELD user ORDER BY user",
            )
            .forEach { assertSame(it, "system") }
    }

    @Test
    fun `returns the same routing table shape`() {
        fun shape(server: DodecaContainer): List<Any?> =
            server.rows("CALL dbms.routing.getRoutingTable({}, 'neo4j')").map { row ->
                val servers =
                    (row.getValue("servers") as List<*>).map {
                        val entry = it as Map<*, *>
                        entry["role"] to (entry["addresses"] as List<*>).size
                    }
                row["ttl"] to servers
            }

        assertThat(shape(dodeca)).isEqualTo(shape(community))
    }

    @Test
    @Order(ADDITIONS)
    fun `creates, stops, starts and drops databases`() {
        refusedByCommunity("CREATE DATABASE films", "system")

        dodeca.rows("CREATE DATABASE films", "system")
        assertThat(statuses()).containsEntry("films", "online")
        dodeca.rows("CREATE (:Film {title: 'Alien'})", "films")
        dodeca.rows("STOP DATABASE films", "system")
        assertThat(statuses()).containsEntry("films", "offline")
        dodeca.rows("START DATABASE films", "system")
        assertThat(dodeca.rows("MATCH (f:Film) RETURN f.title AS title", "films"))
            .containsExactly(mapOf("title" to "Alien"))
        dodeca.rows("DROP DATABASE films", "system")
        assertThat(statuses()).doesNotContainKey("films")
    }

    @Test
    @Order(ADDITIONS)
    fun `waits for database commands`() {
        val rows = dodeca.rows("CREATE DATABASE waiting WAIT 30 SECONDS", "system")

        assertThat(rows.map { it - "address" })
            .containsExactly(
                mapOf("state" to "CaughtUp", "message" to "caught up", "success" to true)
            )
        assertThat(rows.single()["address"] as String).endsWith(":7687")
        assertThat(statuses()).containsEntry("waiting", "online")
    }

    @Test
    @Order(ADDITIONS)
    fun `enforces property existence constraints`() {
        val create = "CREATE CONSTRAINT person_born FOR (p:Person) REQUIRE p.born IS NOT NULL"
        refusedByCommunity(create)
        dodeca.rows("CREATE DATABASE constraints", "system")
        dodeca.rows("CREATE (:Person {name: 'unborn'})", "constraints")

        assertThat(failed(create, "constraints").code)
            .isEqualTo("Neo.DatabaseError.Schema.ConstraintCreationFailed")
        dodeca.rows("MATCH (p:Person) SET p.born = 1900", "constraints")
        dodeca.rows(create, "constraints")
        assertThat(failed("CREATE (:Person {name: 'nobody'})", "constraints").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
    }

    @Test
    @Order(ADDITIONS)
    fun `enforces property type constraints`() {
        val create =
            "CREATE CONSTRAINT person_born_type FOR (p:Person) REQUIRE p.born IS :: INTEGER"
        refusedByCommunity(create)
        dodeca.rows("CREATE DATABASE types", "system")
        dodeca.rows("CREATE (:Person {born: 'long ago'})", "types")

        assertThat(failed(create, "types").code)
            .isEqualTo("Neo.DatabaseError.Schema.ConstraintCreationFailed")
        dodeca.rows("MATCH (p:Person) SET p.born = 1900", "types")
        dodeca.rows(create, "types")
        assertThat(failed("CREATE (:Person {born: 'yesterday'})", "types").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
        assertThat(failed("MATCH (p:Person) SET p.born = 1900.5", "types").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
    }

    @Test
    @Order(ADDITIONS)
    fun `runs graph types`() {
        val set =
            "CYPHER 25 ALTER CURRENT GRAPH TYPE SET { " +
                "(:City => :Place {name :: STRING NOT NULL}) }"
        val show = "CYPHER 25 SHOW CURRENT GRAPH TYPE"
        refusedByCommunity(set)
        refusedByCommunity(show)
        dodeca.rows("CREATE DATABASE graphtypes", "system")

        dodeca.rows(set, "graphtypes")

        assertThat(dodeca.rows(show, "graphtypes").single()["specification"] as String)
            .contains("(:`City` => :`Place` {`name` :: STRING NOT NULL})")
        assertThat(failed("CREATE (:City {name: 'Gent'})", "graphtypes").code)
            .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
        dodeca.rows("CREATE (:City:Place {name: 'Gent'})", "graphtypes")
    }

    @Test
    @Order(ADDITIONS)
    fun `database commands fail with Neo4j's status codes`() {
        dodeca.rows("CREATE DATABASE messages", "system")

        assertThat(failed("CREATE DATABASE messages", "system").code)
            .isEqualTo("Neo.ClientError.Database.ExistingDatabaseFound")
        assertThat(failed("STOP DATABASE missing", "system").code)
            .isEqualTo("Neo.ClientError.Database.DatabaseNotFound")
        assertThat(failed("DROP DATABASE neo4j", "system").message)
            .isEqualTo("Not allowed to delete the default database 'neo4j'.")
    }

    @Test
    @Order(ADDITIONS)
    fun `refuses suspended users and resolves home databases, which Community refuses to set`() {
        refusedByCommunity(
            "CREATE USER carol SET PASSWORD 'carol-secret-1' SET STATUS SUSPENDED",
            "system",
        )
        refusedByCommunity(
            "CREATE USER dan SET PASSWORD 'dan-secret-1' SET HOME DATABASE neo4j",
            "system",
        )
        dodeca.rows("CREATE DATABASE homes", "system")
        dodeca.rows(
            "CREATE USER erin SET PASSWORD 'erin-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE homes",
            "system",
        )
        val home = "CALL db.info() YIELD name"

        assertThat(dodeca.runAs("erin", "erin-secret-1", home))
            .isEqualTo(Outcome.Rows(listOf(mapOf("name" to "homes"))))
        assertThat(
                dodeca.rows("SHOW USERS YIELD user, suspended, home WHERE user = 'erin'", "system")
            )
            .containsExactly(mapOf("user" to "erin", "suspended" to false, "home" to "homes"))

        dodeca.rows("ALTER USER erin SET STATUS SUSPENDED", "system")

        assertThat((dodeca.runAs("erin", "erin-secret-1", home) as Outcome.Failed).code)
            .isEqualTo("Neo.ClientError.Security.Unauthorized")
        dodeca.rows("ALTER USER erin REMOVE HOME DATABASE SET STATUS ACTIVE", "system")
        assertThat(dodeca.runAs("erin", "erin-secret-1", home))
            .isEqualTo(Outcome.Rows(listOf(mapOf("name" to "neo4j"))))
    }

    @Test
    @Order(ADDITIONS)
    fun `gives users the privileges of their roles, which Community refuses`() {
        refusedByCommunity("CREATE ROLE curator", "system")
        refusedByCommunity("SHOW ROLES", "system")
        refusedByCommunity("SHOW PRIVILEGES", "system")
        for (command in
            listOf(
                "CREATE DATABASE archive WAIT",
                "CREATE USER frank SET PASSWORD 'frank-secret-1' CHANGE NOT REQUIRED",
                "CREATE ROLE curator",
                "GRANT ACCESS ON DATABASE archive TO curator",
                "GRANT MATCH {title} ON GRAPH archive NODES Film TO curator",
                "GRANT ROLE curator TO frank",
            )) {
            dodeca.rows(command, "system")
        }
        dodeca.rows(
            "CREATE (:Film {title: 'Alien', budget: 11}), (:Draft {title: 'Untitled'})",
            "archive",
        )

        assertThat(
                dodeca.runAs(
                    "frank",
                    "frank-secret-1",
                    "MATCH (n) RETURN n.title AS title, n.budget AS budget",
                    "archive",
                )
            )
            .isEqualTo(Outcome.Rows(listOf(mapOf("title" to "Alien", "budget" to null))))
        assertThat(dodeca.runAs("frank", "frank-secret-1", "CREATE (:Film)", "archive"))
            .isEqualTo(
                Outcome.Failed(
                    "Neo.ClientError.Security.Forbidden",
                    "Create node with labels 'Film' on database 'archive' is not allowed for " +
                        "user 'frank' with roles [PUBLIC, curator].",
                )
            )
        assertThat(dodeca.runAs("frank", "frank-secret-1", "RETURN 1", "neo4j"))
            .isEqualTo(Outcome.Rows(listOf(mapOf("1" to 1L))))
        assertThat(dodeca.rows("SHOW USERS YIELD user, roles WHERE user = 'frank'", "system"))
            .containsExactly(mapOf("user" to "frank", "roles" to listOf("curator", "PUBLIC")))
        assertThat(
                dodeca.rows("SHOW ROLE curator PRIVILEGES AS COMMANDS", "system").map {
                    it["command"]
                }
            )
            .containsExactly(
                "GRANT ACCESS ON DATABASE archive TO `curator`",
                "GRANT MATCH {title} ON GRAPH archive NODE Film TO `curator`",
            )
    }

    @Test
    @Order(ADDITIONS)
    fun `places a database on one primary and sets its access, which Community refuses`() {
        refusedByCommunity("CREATE DATABASE places TOPOLOGY 1 PRIMARY", "system")
        refusedByCommunity("ALTER DATABASE neo4j SET ACCESS READ ONLY", "system")
        dodeca.rows("CREATE DATABASE places TOPOLOGY 1 PRIMARY WAIT", "system")

        dodeca.rows("ALTER DATABASE places SET ACCESS READ ONLY", "system")

        assertThat(
                dodeca.rows("SHOW DATABASE places YIELD requestedPrimariesCount, access", "system")
            )
            .containsExactly(mapOf("requestedPrimariesCount" to 1L, "access" to "read-only"))
        assertThat(failed("CREATE (:Place)", "places").message).contains("read-only")
        assertThat(failed("CREATE DATABASE wide TOPOLOGY 2 PRIMARIES", "system").message)
            .contains("runs without clustering")
        // Secondaries pull from other servers, which a server without clustering does not have.
        val noSecondaries = "can host it as a secondary, and this server runs without clustering"
        assertThat(failed("CREATE DATABASE read TOPOLOGY 1 PRIMARY 1 SECONDARY", "system").message)
            .contains(noSecondaries)
        assertThat(
                failed("ALTER DATABASE places SET TOPOLOGY 1 PRIMARY 2 SECONDARIES", "system")
                    .message
            )
            .contains(noSecondaries)
        assertThat(dodeca.rows("SHOW DATABASE places YIELD requestedSecondariesCount", "system"))
            .containsExactly(mapOf("requestedSecondariesCount" to 0L))
    }

    @Test
    @Order(ADDITIONS)
    fun `serves databases by their aliases, which Community refuses to create`() {
        refusedByCommunity("CREATE ALIAS motion FOR DATABASE neo4j", "system")
        refusedByCommunity("SHOW ALIASES FOR DATABASE", "system")
        dodeca.rows("CREATE DATABASE pictures WAIT", "system")
        dodeca.rows("CREATE ALIAS motion FOR DATABASE pictures", "system")
        dodeca.rows("CREATE (:Film {title: 'Alien'})", "motion")

        assertThat(dodeca.rows("SHOW ALIASES FOR DATABASE", "system"))
            .containsExactly(
                mapOf(
                    "name" to "motion",
                    "composite" to null,
                    "database" to "pictures",
                    "location" to "local",
                    "url" to null,
                    // Cypher 25's, which the image's neo4j.conf makes the default.
                    "credentials" to null,
                    "user" to null,
                )
            )
        assertThat(dodeca.rows("MATCH (f:Film) RETURN f.title AS title", "pictures"))
            .containsExactly(mapOf("title" to "Alien"))
    }

    @Test
    @Order(ADDITIONS)
    fun `gives a database a default Cypher version when creating it, which Community refuses`() {
        refusedByCommunity("CREATE DATABASE versions DEFAULT LANGUAGE CYPHER 5", "system")
        dodeca.rows("CREATE DATABASE versions DEFAULT LANGUAGE CYPHER 5 WAIT", "system")
        // Cypher 25 is the default, as the image's neo4j.conf makes it, but not this database's.
        fun language() =
            dodeca
                .rows(
                    "SHOW FUNCTIONS YIELD name WHERE name = 'coll.distinct' RETURN count(*) AS n",
                    "versions",
                )
                .single()["n"]
        fun shown() = dodeca.rows("SHOW DATABASE versions YIELD defaultLanguage", "system").single()

        assertThat(shown()).isEqualTo(mapOf("defaultLanguage" to "CYPHER 5"))
        await atMost Duration.ofSeconds(30) untilAsserted { assertThat(language()).isEqualTo(0L) }

        dodeca.rows("ALTER DATABASE versions SET DEFAULT LANGUAGE CYPHER 25", "system")

        assertThat(shown()).isEqualTo(mapOf("defaultLanguage" to "CYPHER 25"))
        await atMost Duration.ofSeconds(30) untilAsserted { assertThat(language()).isEqualTo(1L) }
    }

    private companion object {
        /** About a kilobyte of text, to fill a disk with few rows. */
        val PADDING = "x".repeat(1000)

        /** Run last: they leave the servers' databases different. */
        const val ADDITIONS = Int.MAX_VALUE
    }
}
