package org.dozendb.it

import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpRequest.BodyPublishers
import java.net.http.HttpResponse.BodyHandlers
import java.time.Duration
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.UUID
import javax.crypto.Mac
import javax.crypto.spec.SecretKeySpec
import org.assertj.core.api.Assertions.assertThat
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.dozendb.consensus.testing.TestAuthority
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.EnumSource
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.lifecycle.Startables
import org.testcontainers.utility.DockerImageName

/**
 * Cloud storage through the plugin jar, against emulators of Amazon S3 (S3Mock), Google Cloud
 * Storage (fake-gcs-server) and Azure Blob Storage (Azurite), which servers and `neo4j-admin` reach
 * with each SDK's own variables: LOAD CSV, dumps and loads, backups, restores and seeds.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class CloudStorageIT {
    /** A bucket, or Azure container, `films`, in each cloud. */
    enum class Cloud(val bucket: String) {
        S3("s3://films"),
        GCS("gs://films"),
        AZURE("azb://$ACCOUNT/films"),
    }

    private val network = Network.newNetwork()
    private val run = UUID.randomUUID().toString().take(8)
    private val authority = TestAuthority("DozenDB IT cloud CA")
    private val containers = mutableListOf<GenericContainer<*>>()
    private val volumes = mutableListOf<String>()

    private val s3 =
        GenericContainer(DockerImageName.parse(System.getProperty("s3mock.image")))
            .withNetwork(network)
            .withNetworkAliases("s3")
            .withExposedPorts(9090)
    private val gcs =
        GenericContainer(DockerImageName.parse(System.getProperty("fake-gcs.image")))
            .withNetwork(network)
            .withNetworkAliases("gcs")
            .withExposedPorts(4443)
            // Resumable uploads continue at this URL, which the servers reach.
            .withCommand("-scheme", "http", "-port", "4443", "-external-url", "http://gcs:4443")
    private val azure =
        GenericContainer(DockerImageName.parse(System.getProperty("azurite.image")))
            .withNetwork(network)
            .withNetworkAliases("azb")
            .withExposedPorts(10000)
            .withCommand(
                "azurite-blob",
                "--blobHost",
                "0.0.0.0",
                "--skipApiVersionCheck",
                "--loose",
            )
    private val http = HttpClient.newHttpClient()

    @BeforeAll
    fun start() {
        Startables.deepStart(s3, gcs, azure).join()
        send(
            HttpRequest.newBuilder(URI("${endpoint(s3, 9090)}/films")).PUT(BodyPublishers.noBody())
        )
        send(
            HttpRequest.newBuilder(URI("${endpoint(gcs, 4443)}/storage/v1/b?project=dozendb"))
                .header("Content-Type", "application/json")
                .POST(BodyPublishers.ofString("""{"name": "films"}"""))
        )
        send(azurite("films?restype=container", BodyPublishers.noBody(), ""))
        put("csv/people.csv", "name,age\nAlice,30\nBob,40\nCarol,50\n")
    }

    @AfterAll
    fun stopClouds() {
        listOf(s3, gcs, azure).forEach { it.stop() }
        network.close()
    }

    @AfterEach
    fun stop() {
        containers.forEach {
            if (it is DozenDbContainer) {
                it.saveLogs(DozenDbContainer.logsDirectory.resolve(javaClass.simpleName))
            }
            it.stop()
        }
        containers.clear()
        volumes.forEach(DozenDbContainer::removeVolume)
        volumes.clear()
    }

    private fun endpoint(container: GenericContainer<*>, port: Int) =
        "http://${container.host}:${container.getMappedPort(port)}"

    private fun send(request: HttpRequest.Builder) {
        val answer = http.send(request.build(), BodyHandlers.ofString())
        check(answer.statusCode() in 200..299) { "${answer.statusCode()}: ${answer.body()}" }
    }

    /**
     * A PUT of [body], [length] bytes long, to [path] in Azurite's account, signed with its key
     * (Shared Key), as Azure's SDK would sign it.
     */
    private fun azurite(
        path: String,
        body: HttpRequest.BodyPublisher,
        length: String,
    ): HttpRequest.Builder {
        val date = DateTimeFormatter.RFC_1123_DATE_TIME.format(ZonedDateTime.now(ZoneOffset.UTC))
        val headers = sortedMapOf("x-ms-date" to date, "x-ms-version" to "2021-08-06")
        if (!path.contains("restype=container")) headers["x-ms-blob-type"] = "BlockBlob"
        val (resource, query) = path.split('?').let { it[0] to it.getOrNull(1) }
        val canonical =
            "/$ACCOUNT/$ACCOUNT/$resource" +
                (query?.split('&')?.sorted()?.joinToString("") { "\n" + it.replace('=', ':') }
                    ?: "")
        val signed =
            listOf("PUT", "", "", length, "", "", "", "", "", "", "", "").joinToString("\n") +
                "\n" +
                headers.entries.joinToString("") { "${it.key}:${it.value}\n" } +
                canonical
        val mac = Mac.getInstance("HmacSHA256")
        mac.init(SecretKeySpec(Base64.getDecoder().decode(KEY), "HmacSHA256"))
        val signature = Base64.getEncoder().encodeToString(mac.doFinal(signed.toByteArray()))
        return HttpRequest.newBuilder(URI("${endpoint(azure, 10000)}/$ACCOUNT/$path"))
            .apply { headers.forEach { (name, value) -> header(name, value) } }
            .header("Authorization", "SharedKey $ACCOUNT:$signature")
            .PUT(body)
    }

    /** Puts [text] at [key] in each cloud's `films`. */
    private fun put(key: String, text: String) {
        send(
            HttpRequest.newBuilder(URI("${endpoint(s3, 9090)}/films/$key"))
                .PUT(BodyPublishers.ofString(text))
        )
        send(
            HttpRequest.newBuilder(
                    URI(
                        "${endpoint(gcs, 4443)}/upload/storage/v1/b/films/o?uploadType=media&" +
                            "name=$key"
                    )
                )
                .POST(BodyPublishers.ofString(text))
        )
        val bytes = text.toByteArray()
        send(azurite("films/$key", BodyPublishers.ofByteArray(bytes), "${bytes.size}"))
    }

    private fun volume(name: String): String = "dozendb-it-$run-$name".also { volumes += it }

    /** What a DozenDB server or `neo4j-admin` needs to reach the clouds, as each SDK reads it. */
    private fun <T : GenericContainer<*>> T.reachingClouds(azureKey: String = KEY): T {
        withEnv("AWS_ACCESS_KEY_ID", "key")
        withEnv("AWS_SECRET_ACCESS_KEY", "secret")
        withEnv("AWS_REGION", "eu-west-3")
        withEnv("AWS_ENDPOINT_URL_S3", "http://s3:9090")
        withEnv("STORAGE_EMULATOR_HOST", "http://gcs:4443")
        withEnv("AZURE_STORAGE_CONNECTION_STRING", connectionString(azureKey, "http://azb:10000"))
        return this
    }

    private fun server(
        alias: String,
        configure: DozenDbContainer.() -> Unit = {},
    ): DozenDbContainer =
        DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
            .inNetwork(network)
            .reachingClouds()
            .apply(configure)
            .also {
                containers += it
                it.start()
            }

    private fun admin(
        alias: String,
        volumes: Map<String, String> = emptyMap(),
        azureKey: String = KEY,
    ): AdminContainer =
        AdminContainer(DozenDbContainer.dozenDbImage, alias, network, volumes)
            .reachingClouds(azureKey)
            .also {
                containers += it
                it.start()
            }

    private fun count(server: DozenDbContainer, label: String, database: String = "neo4j"): Long =
        server.rows("MATCH (n:$label) RETURN count(n) AS c", database).single()["c"] as Long

    /** Runs `neo4j-admin` with [arguments] in [server], giving what it writes to Neo4j's user. */
    private fun adminIn(server: DozenDbContainer, vararg arguments: String): String {
        val result =
            server.execInContainer(
                "sh",
                "-c",
                "neo4j-admin ${arguments.joinToString(" ")} 2>&1; status=\$?; " +
                    "chown -R 7474:7474 /data; exit \$status",
            )
        check(result.exitCode == 0) {
            "neo4j-admin ${arguments.joinToString(" ")}: ${result.stdout}"
        }
        return result.stdout
    }

    @ParameterizedTest
    @EnumSource(Cloud::class)
    fun `LOAD CSV reads, and dumps go to and load from, cloud storage`(cloud: Cloud) {
        val server = server("csv-${cloud.name.lowercase()}")
        val folder = "${cloud.bucket}/dumps-$run/"

        assertThat(
                server
                    .rows(
                        "LOAD CSV WITH HEADERS FROM '${cloud.bucket}/csv/people.csv' AS row " +
                            "RETURN collect(row.name) AS names"
                    )
                    .single()["names"]
            )
            .isEqualTo(listOf("Alice", "Bob", "Carol"))
        // Random values, so that the dump is sent in several parts.
        server.rows(
            "UNWIND range(1, 300000) AS i CALL { WITH i CREATE (:Item {a: randomUUID(), " +
                "b: randomUUID()}) } IN TRANSACTIONS OF 50000 ROWS"
        )
        server.rows("STOP DATABASE neo4j WAIT", "system")
        assertThat(adminIn(server, "database", "dump", "neo4j", "--to-path=$folder"))
            .contains("Dump completed successfully")
        server.rows("START DATABASE neo4j WAIT", "system")
        server.rows("MATCH (n) CALL { WITH n DETACH DELETE n } IN TRANSACTIONS")
        server.rows("STOP DATABASE neo4j WAIT", "system")
        adminIn(
            server,
            "database",
            "load",
            "neo4j",
            "--from-path=$folder",
            "--overwrite-destination=true",
        )
        server.rows("START DATABASE neo4j WAIT", "system")

        assertThat(count(server, "Item")).isEqualTo(300_000)
    }

    @ParameterizedTest
    @EnumSource(Cloud::class)
    fun `a chain of backups in cloud storage is inspected, checked, restored and aggregated`(
        cloud: Cloud
    ) {
        val name = cloud.name.lowercase()
        val source = server("src-$name") { backupServer(authority) }
        source.rows("UNWIND range(1, 2000) AS i CREATE (:Item {i: i})-[:NEXT]->(:Item {i: -i})")
        val folder = "${cloud.bucket}/backups-$run/"
        val client =
            admin("client-$name").backupClient(authority.issue(names = listOf("client-$name")))
        client.shell("mkdir -p /tmp/backup /tmp/check /tmp/aggregate")
        fun backup(vararg arguments: String) =
            client.adminOk(
                "database",
                "backup",
                "--from=src-$name:${DozenDbContainer.BACKUP_PORT}",
                "--to-path=$folder",
                "--temp-path=/tmp/backup",
                *arguments,
                "neo4j",
            )
        assertThat(backup("--type=FULL")).contains("to $folder", ": a full backup up to")
        source.rows("CREATE (:Later)")
        assertThat(backup()).contains(": a differential backup of transaction")

        assertThat(client.adminOk("backup", "inspect", folder).lines().filter { "$folder" in it })
            .hasSize(2)
        client.adminOk(
            "database",
            "check",
            "--from-path=$folder",
            "--temp-path=/tmp/check",
            "neo4j",
        )
        val restored = volume("restored-$name")
        admin("restorer-$name", mapOf(restored to "/data")).apply {
            adminOk("database", "restore", "--from-path=$folder")
            shell("chown -R 7474:7474 /data")
        }
        val target = server("target-$name") { withDataVolume(restored) }
        assertThat(count(target, "Item")).isEqualTo(4000)
        assertThat(count(target, "Later")).isEqualTo(1)

        assertThat(
                client.adminOk(
                    "database",
                    "aggregate-backup",
                    "--from-path=$folder",
                    "--temp-path=/tmp/aggregate",
                    "neo4j",
                )
            )
            .contains("Aggregated the chain of 2 backups of 'neo4j' into $folder")
        assertThat(client.adminOk("backup", "inspect", folder).lines().filter { "$folder" in it })
            .hasSize(1)
    }

    @Test
    fun `a cluster creates databases from seeds in each cloud`() {
        val source = server("seeder") { backupServer(authority) }
        source.rows("UNWIND range(1, 1000) AS i CREATE (:Item {i: i})")
        val client =
            admin("seeder-client").backupClient(authority.issue(names = listOf("seeder-client")))
        client.shell("mkdir -p /tmp/backup")
        for (cloud in Cloud.entries) {
            client.adminOk(
                "database",
                "backup",
                "--from=seeder:${DozenDbContainer.BACKUP_PORT}",
                "--to-path=${cloud.bucket}/seeds-$run/",
                "--temp-path=/tmp/backup",
                "neo4j",
            )
        }
        val aliases = listOf("core1", "core2", "core3")
        val members = aliases.map { alias ->
            DozenDbContainer(DozenDbContainer.dozenDbImage, alias)
                .inNetwork(network)
                .clusterMember(aliases.map { "$it:7700" })
                .reachingClouds()
                .also { containers += it }
        }
        Startables.deepStart(members).join()

        for (cloud in Cloud.entries) {
            val database = "films${cloud.name.lowercase()}"
            // A folder of backups: its latest chain seeds the database.
            val waited =
                administer(
                    members,
                    "CREATE DATABASE $database OPTIONS {existingData: 'use', seedURI: " +
                        "'${cloud.bucket}/seeds-$run/'} WAIT",
                )
            assertThat(waited).describedAs("$database: $waited").allMatch { it["success"] == true }
            for (member in members) {
                assertThat(
                        member.rows(
                            "SHOW DATABASE $database YIELD currentStatus",
                            "system",
                        )
                    )
                    .allMatch { it["currentStatus"] == "online" }
            }
        }
    }

    /**
     * Runs the administration command [cypher] on the leader of `system`, whichever of [members] it
     * is once there is one, and returns its rows: the others refuse it without running it.
     */
    private fun administer(
        members: List<DozenDbContainer>,
        cypher: String,
    ): List<Map<String, Any?>> {
        var rows: List<Map<String, Any?>>? = null
        await withPollInterval
            Duration.ofSeconds(1) atMost
            Duration.ofMinutes(2) untilAsserted
            {
                rows = members.firstNotNullOfOrNull {
                    (it.run(cypher, "system") as? Outcome.Rows)?.rows
                }
                assertThat(rows).describedAs(cypher).isNotNull
            }
        return rows!!
    }

    @Test
    fun `refusals of cloud storage say what the cloud said`() {
        val server = server("refused") { reachingClouds(azureKey = "d3Jvbmc=") }
        val client = admin("refused-client", azureKey = "d3Jvbmc=")
        client.shell("mkdir -p /tmp/backup")

        val denied =
            client.admin(
                "database",
                "backup",
                "--from=refused:${DozenDbContainer.BACKUP_PORT}",
                "--to-path=${Cloud.AZURE.bucket}/denied/",
                "--temp-path=/tmp/backup",
                "neo4j",
            )
        assertThat(denied.exitCode).isNotZero
        assertThat(denied.stderr)
            .contains(
                "Cannot list --to-path ${Cloud.AZURE.bucket}/denied/: " +
                    "${Cloud.AZURE.bucket}/denied/: Server failed to authenticate the request"
            )
        val missing =
            client.admin(
                "database",
                "backup",
                "--from=refused:${DozenDbContainer.BACKUP_PORT}",
                "--to-path=s3://missing/backups/",
                "--temp-path=/tmp/backup",
                "neo4j",
            )
        assertThat(missing.stderr)
            .contains("Cannot list --to-path s3://missing/backups/: s3://missing/backups/:")
            .contains("(NoSuchBucket)")
        server.rows("STOP DATABASE neo4j WAIT", "system")
        val dump =
            server.execInContainer(
                "neo4j-admin",
                "database",
                "dump",
                "neo4j",
                "--verbose",
                "--to-path=${Cloud.AZURE.bucket}/denied/",
            )
        assertThat(dump.exitCode).isNotZero
        // The dump first asks whether its file exists: Azure answers a HEAD request without a
        // body, so its error code is all it says.
        assertThat(dump.stdout + dump.stderr)
            .contains(
                "AccessDeniedException: ${Cloud.AZURE.bucket}/denied/neo4j.dump: " +
                    "AuthorizationFailure"
            )
    }

    private companion object {
        /** Azurite's account and its key, which Azure's documentation publishes. */
        const val ACCOUNT = "devstoreaccount1"
        const val KEY =
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="

        fun connectionString(key: String, endpoint: String) =
            "DefaultEndpointsProtocol=http;AccountName=$ACCOUNT;AccountKey=$key;BlobEndpoint=" +
                "$endpoint/$ACCOUNT;"
    }
}
