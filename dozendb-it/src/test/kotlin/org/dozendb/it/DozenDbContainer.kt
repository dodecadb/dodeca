package org.dozendb.it

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.ExposedPort
import com.github.dockerjava.api.model.Volume
import java.io.ByteArrayOutputStream
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.net.http.HttpResponse.BodyHandlers
import java.nio.file.Files
import java.nio.file.Path
import java.security.KeyStore
import java.time.Duration
import java.util.Base64
import javax.crypto.SecretKey
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.containers.startupcheck.OneShotStartupCheckStrategy
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.images.builder.ImageFromDockerfile
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName

/** The outcome of one query, comparable across servers. */
sealed interface Outcome {
    data class Rows(val rows: List<Map<String, Any?>>) : Outcome

    data class Failed(val code: String, val message: String) : Outcome
}

/** A Neo4j server in Docker: the official Community image, or that image with the DozenDB jar. */
class DozenDbContainer(image: DockerImageName, val alias: String) :
    GenericContainer<DozenDbContainer>(image) {
    init {
        withNetworkAliases(alias)
        withCreateContainerCmdModifier { it.withHostName(alias) }
        withEnv("NEO4J_AUTH", "neo4j/$PASSWORD")
        withEnv("NEO4J_server_memory_heap_initial__size", "512m")
        withEnv("NEO4J_server_memory_heap_max__size", "512m")
        withEnv("NEO4J_server_memory_pagecache_size", "64m")
        withEnv("NEO4J_server_default__advertised__address", alias)
        // A reply lost on its way, as OrbStack's port forwarding loses some at times, fails the
        // request after 10 s instead of waiting for ever: the server tells drivers to close a
        // connection that waits that long for a reply (keep-alive times probes), and keeps the
        // connections of long requests from going quiet.
        withEnv("NEO4J_server_bolt_connection__keep__alive", "5s")
        withEnv("NEO4J_server_bolt_connection__keep__alive__streaming__scheduling__interval", "1s")
        withExposedPorts(BOLT_PORT)
        waitingFor(
            Wait.forLogMessage(".*Started\\.\\s*", 1).withStartupTimeout(Duration.ofMinutes(3))
        )
    }

    private var driver: Driver? = null

    fun inNetwork(network: Network): DozenDbContainer = withNetwork(network)

    /**
     * Makes this server a cluster member that forms a cluster with [initialMembers]. Members talk
     * to each other over mutual TLS with hostname verification, with a certificate for [alias] that
     * [authority] issued, or in plaintext if it is null.
     */
    fun clusterMember(
        initialMembers: List<String>,
        authority: TestAuthority? = clusterAuthority,
    ): DozenDbContainer {
        withEnv("NEO4J_dbms_dozendb_cluster_enabled", "true")
        withEnv("NEO4J_dbms_dozendb_cluster_initial__members", initialMembers.joinToString(","))
        // Tests about other things keep their leaders; see [balancingLeaders].
        withEnv(BALANCING_STRATEGY, "NO_BALANCING")
        withEnv("NEO4J_dbms_ssl_policy_cluster_enabled", (authority != null).toString())
        if (authority != null) {
            withEnv("NEO4J_dbms_ssl_policy_cluster_verify__hostname", "true")
            authority.issue(names = listOf(alias)).files().forEach { (path, text) ->
                withCopyToContainer(
                    Transferable.of(text.toByteArray(), READABLE),
                    "$CLUSTER_CERTIFICATES/$path",
                )
            }
        }
        return this
    }

    /**
     * Balances leaders as members do by default, after leaders have stayed put for [delay] instead
     * of 10 s.
     */
    fun balancingLeaders(delay: Duration): DozenDbContainer {
        withEnv(BALANCING_STRATEGY, "EQUAL_BALANCING")
        return withEnv(
            "NEO4J_internal_dbms_dozendb_cluster_leadership__balancing__delay",
            "${delay.toMillis()}ms",
        )
    }

    /**
     * Serves backups on every interface over TLS, with a certificate for [alias] that [authority]
     * issued; clients need a certificate from it too.
     */
    fun backupServer(authority: TestAuthority): DozenDbContainer {
        withEnv("NEO4J_server_backup_listen__address", "0.0.0.0:$BACKUP_PORT")
        withEnv("NEO4J_dbms_ssl_policy_backup_enabled", "true")
        authority.issue(names = listOf(alias)).files().forEach { (path, text) ->
            withCopyToContainer(
                Transferable.of(text.toByteArray(), READABLE),
                "$BACKUP_CERTIFICATES/$path",
            )
        }
        return this
    }

    /**
     * Serves Bolt over TLS as well as in plaintext, with a certificate for [names] that [authority]
     * issued, as remote aliases with `+s` schemes reach it.
     */
    fun boltTls(authority: TestAuthority, names: List<String> = listOf(alias)): DozenDbContainer {
        withEnv("NEO4J_dbms_ssl_policy_bolt_enabled", "true")
        withEnv("NEO4J_dbms_ssl_policy_bolt_client__auth", "NONE")
        withEnv("NEO4J_server_bolt_tls__level", "OPTIONAL")
        authority.issue(names = names).files().forEach { (path, text) ->
            withCopyToContainer(
                Transferable.of(text.toByteArray(), READABLE),
                "$BOLT_CERTIFICATES/$path",
            )
        }
        return this
    }

    /**
     * Trusts the certificates that [authority] issues, through the JVM's trust store as the
     * operator guide has it for remote aliases with `+s` schemes: a JKS store, which the JVM reads
     * without a password.
     */
    fun trusting(authority: TestAuthority): DozenDbContainer {
        val store =
            KeyStore.getInstance("JKS").apply {
                load(null, null)
                setCertificateEntry("test-ca", authority.certificate)
            }
        val bytes = ByteArrayOutputStream().also { store.store(it, CharArray(0)) }.toByteArray()
        withCopyToContainer(Transferable.of(bytes, READABLE), TRUST_STORE)
        return withEnv("NEO4J_server_jvm_additional", "-Djavax.net.ssl.trustStore=$TRUST_STORE")
    }

    /** Encrypts the passwords of remote aliases with [key], from a keystore at [path]. */
    fun aliasKeystore(key: SecretKey, path: String = ALIAS_KEYSTORE): DozenDbContainer {
        withCopyToContainer(Transferable.of(keystore(key), READABLE), path)
        withEnv("NEO4J_dbms_security_keystore_path", path)
        withEnv("NEO4J_dbms_security_keystore_password", ALIAS_KEYSTORE_PASSWORD)
        return withEnv("NEO4J_dbms_security_key_name", ALIAS_KEY_NAME)
    }

    /**
     * Publishes Bolt on [hostPort] of this machine and advertises it there, so routing tables lead
     * drivers outside the Docker network to this member.
     */
    fun boltOnHostPort(hostPort: Int): DozenDbContainer {
        setPortBindings(portBindings + "$hostPort:$BOLT_PORT")
        return withEnv("NEO4J_server_bolt_advertised__address", "127.0.0.1:$hostPort")
    }

    /** Publishes Neo4j's HTTP connector, for [post]. */
    fun withHttp(): DozenDbContainer {
        addExposedPort(HTTP_PORT)
        return this
    }

    /** Posts [json] to [path] of Neo4j's HTTP API as `neo4j`. */
    fun post(path: String, json: String): HttpResponse<String> =
        http.send(
            HttpRequest.newBuilder(URI("http://$host:${getMappedPort(HTTP_PORT)}$path"))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .header(
                    "Authorization",
                    "Basic " + Base64.getEncoder().encodeToString("neo4j:$PASSWORD".toByteArray()),
                )
                .POST(HttpRequest.BodyPublishers.ofString(json))
                .build(),
            BodyHandlers.ofString(),
        )

    /**
     * Reports the metrics that [filter] selects on the Prometheus endpoint, published on [hostPort]
     * of this machine, which stays the same across restarts.
     */
    fun prometheusOnHostPort(hostPort: Int, filter: String = "*"): DozenDbContainer {
        addExposedPort(PROMETHEUS_PORT)
        setPortBindings(portBindings + "$hostPort:$PROMETHEUS_PORT")
        prometheusPort = hostPort
        withEnv("NEO4J_server_metrics_prometheus_enabled", "true")
        withEnv("NEO4J_server_metrics_prometheus_endpoint", "0.0.0.0:$PROMETHEUS_PORT")
        return withEnv("NEO4J_server_metrics_filter", filter)
    }

    private var prometheusPort: Int? = null

    /** The text the Prometheus endpoint reports now. */
    fun scrape(): String {
        val port = checkNotNull(prometheusPort) { "$alias reports no metrics" }
        val response =
            http.send(
                HttpRequest.newBuilder(URI("http://$host:$port/metrics")).build(),
                BodyHandlers.ofString(),
            )
        check(response.statusCode() == 200) { "$alias: HTTP ${response.statusCode()}" }
        return response.body()
    }

    /** The samples the Prometheus endpoint reports now, by name with labels. */
    fun samples(): Map<String, Double> = Promtool.samples(scrape())

    /**
     * The port of this machine that Bolt is published on now: Docker may publish it on another one
     * when the container starts again, unless [boltOnHostPort] fixed it.
     */
    private fun boltPort(): Int =
        DockerClientFactory.instance()
            .client()
            .inspectContainerCmd(containerId)
            .exec()
            .networkSettings
            .ports
            .bindings
            .getValue(ExposedPort.tcp(BOLT_PORT))
            .first()
            .hostPortSpec
            .toInt()

    private fun driver(): Driver =
        driver
            ?: GraphDatabase.driver(
                    "bolt://$host:${boltPort()}",
                    AuthTokens.basic("neo4j", PASSWORD),
                )
                .also { driver = it }

    /** Runs [cypher] with [parameters] against [database] and returns the rows or the error. */
    fun run(
        cypher: String,
        database: String = "neo4j",
        parameters: Map<String, Any?> = emptyMap(),
    ): Outcome =
        try {
            driver().session(SessionConfig.forDatabase(database)).use { session ->
                Outcome.Rows(session.run(cypher, parameters).list { it.asMap() })
            }
        } catch (e: Neo4jException) {
            Outcome.Failed(e.code(), e.message.orEmpty())
        }

    /**
     * Runs [cypher] as [user] in a new connection, against [database] or, without one, the user's
     * home database, and returns the rows or the error.
     */
    fun runAs(user: String, password: String, cypher: String, database: String? = null): Outcome =
        GraphDatabase.driver(
                "bolt://$host:${boltPort()}",
                AuthTokens.basic(user, password),
            )
            .use { driver ->
                val config =
                    if (database == null) SessionConfig.defaultConfig()
                    else SessionConfig.forDatabase(database)
                try {
                    driver.session(config).use { session ->
                        Outcome.Rows(session.run(cypher).list { it.asMap() })
                    }
                } catch (e: Neo4jException) {
                    Outcome.Failed(e.code(), e.message.orEmpty())
                }
            }

    /** Like [run] but fails the test on an error. */
    fun rows(
        cypher: String,
        database: String = "neo4j",
        parameters: Map<String, Any?> = emptyMap(),
    ): List<Map<String, Any?>> =
        when (val outcome = run(cypher, database, parameters)) {
            is Outcome.Rows -> outcome.rows
            is Outcome.Failed -> throw AssertionError("$alias: $cypher failed: $outcome")
        }

    /** Neo4j's debug log, read from the container. */
    fun debugLog(): String = logFile("debug.log")

    /** The log file [name], such as `query.log`, read from the container. */
    fun logFile(name: String): String =
        copyFileFromContainer("/logs/$name") { it.readAllBytes().decodeToString() }

    /** Saves the console and debug logs under [directory] for diagnosis. */
    fun saveLogs(directory: Path) {
        val target = Files.createDirectories(directory.resolve(alias))
        Files.writeString(target.resolve("neo4j.log"), logs)
        // Docker copies files from stopped containers too; not from removed ones.
        runCatching { debugLog() }.onSuccess { Files.writeString(target.resolve("debug.log"), it) }
    }

    private var dataVolume: String? = null
    private val binds = mutableListOf<Bind>()

    init {
        withCreateContainerCmdModifier { it.hostConfig!!.withBinds(binds) }
    }

    /** Keeps `/data` in the Docker volume [name], so that it survives restarts and can be wiped. */
    fun withDataVolume(name: String): DozenDbContainer {
        dataVolume = name
        binds += Bind(name, DATA)
        return this
    }

    /** Mounts the Docker volume [name] at [path]. */
    fun withVolume(name: String, path: String): DozenDbContainer {
        binds += Bind(name, Volume(path))
        return this
    }

    /** Kills the container's process abruptly, keeping the container for its logs. */
    fun kill() {
        driver?.close()
        driver = null
        DockerClientFactory.instance().client().killContainerCmd(containerId).exec()
    }

    /** Stops the server gracefully, keeping the container and its data. */
    fun shutDown() {
        driver?.close()
        driver = null
        DockerClientFactory.instance()
            .client()
            .stopContainerCmd(containerId)
            .withTimeout(SHUTDOWN_SECONDS)
            .exec()
    }

    /** Starts the stopped or killed container again and waits until it answers on Bolt. */
    fun restart() {
        DockerClientFactory.instance().client().startContainerCmd(containerId).exec()
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(3) untilAsserted
            {
                assertThat(run("SHOW DATABASES YIELD name", "system"))
                    .describedAs("$alias answering")
                    .isInstanceOf(Outcome.Rows::class.java)
            }
    }

    /** Deletes [database] from the data volume of this stopped member. */
    fun wipeDatabase(database: String) {
        val volume = checkNotNull(dataVolume) { "$alias keeps its data in the container" }
        GenericContainer(dockerImageName)
            .withCreateContainerCmdModifier {
                it.withEntrypoint(
                    "rm",
                    "-rf",
                    "/data/databases/$database",
                    "/data/transactions/$database",
                )
                it.hostConfig!!.withBinds(Bind(volume, DATA))
            }
            .withStartupCheckStrategy(OneShotStartupCheckStrategy())
            .use { it.start() }
    }

    override fun stop() {
        driver?.close()
        driver = null
        super.stop()
    }

    companion object {
        const val BOLT_PORT = 7687
        const val HTTP_PORT = 7474
        const val ROUTING_PORT = 7688
        const val PROMETHEUS_PORT = 2004
        const val BACKUP_PORT = 6362
        const val PASSWORD = "dozendb-it-secret"
        private const val BALANCING_STRATEGY =
            "NEO4J_dbms_cluster_raft_leader__transfer_balancing__strategy"
        private const val SHUTDOWN_SECONDS = 120
        private val DATA = Volume("/data")
        private val http = HttpClient.newHttpClient()

        /** Where Neo4j's image looks for the cluster SSL policy's certificates. */
        private const val CLUSTER_CERTIFICATES = "/var/lib/neo4j/certificates/cluster"

        /** Where Neo4j's image looks for the backup SSL policy's certificates. */
        const val BACKUP_CERTIFICATES = "/var/lib/neo4j/certificates/backup"
        private const val BOLT_CERTIFICATES = "/var/lib/neo4j/certificates/bolt"
        private const val TRUST_STORE = "/var/lib/neo4j/certificates/trust.jks"

        /** Where [aliasKeystore] puts the keystore by default. */
        const val ALIAS_KEYSTORE = "/var/lib/neo4j/certificates/aliases.p12"
        const val ALIAS_KEYSTORE_PASSWORD = "dozendb-it-keystore"
        const val ALIAS_KEY_NAME = "aliases"

        /** A PKCS12 keystore holding [key], as `keytool -genseckey` makes it. */
        fun keystore(key: SecretKey): ByteArray {
            val store = KeyStore.getInstance("PKCS12").apply { load(null, null) }
            store.setEntry(
                ALIAS_KEY_NAME,
                KeyStore.SecretKeyEntry(key),
                KeyStore.PasswordProtection(ALIAS_KEYSTORE_PASSWORD.toCharArray()),
            )
            return ByteArrayOutputStream()
                .also { store.store(it, ALIAS_KEYSTORE_PASSWORD.toCharArray()) }
                .toByteArray()
        }

        /** File mode 0644. */
        const val READABLE = 420

        /** Issues the certificates of cluster members. */
        val clusterAuthority = TestAuthority("DozenDB IT CA")

        /** Removes the Docker volume [name], once no container uses it. */
        fun removeVolume(name: String) {
            DockerClientFactory.instance().client().removeVolumeCmd(name).exec()
        }

        private fun property(name: String): String =
            requireNotNull(System.getProperty(name)) {
                "system property $name is not set; run with -Pit"
            }

        /** Neo4j's official Community image, which DozenDB is compared with and installed into. */
        val communityImage: DockerImageName by lazy {
            DockerImageName.parse(property("neo4j.image"))
        }

        /** The Community image with the DozenDB jar in `lib/`. */
        val dozenDbImage: DockerImageName by lazy {
            val jar = Path.of(property("dozendb.plugin.jar"))
            check(Files.isRegularFile(jar)) { "$jar does not exist; build dozendb-plugin first" }
            val image =
                ImageFromDockerfile("dozendb-it", true)
                    .withFileFromPath("dozendb.jar", jar)
                    .withDockerfileFromBuilder {
                        it.from(communityImage.asCanonicalNameString())
                            .copy("dozendb.jar", "/var/lib/neo4j/lib/dozendb.jar")
                            .build()
                    }
            DockerImageName.parse(image.get())
        }

        val logsDirectory: Path by lazy { Path.of(property("it.logs")) }
    }
}
