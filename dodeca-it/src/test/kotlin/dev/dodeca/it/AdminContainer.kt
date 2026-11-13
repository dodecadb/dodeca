package dev.dodeca.it

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.Volume
import dev.dodeca.consensus.testing.TestCertificate
import org.testcontainers.containers.Container.ExecResult
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.Network
import org.testcontainers.images.builder.Transferable
import org.testcontainers.utility.DockerImageName

/**
 * A container of [image] that runs `neo4j-admin` and shell commands on demand, as root, in
 * [network] as [alias], with Docker volumes mounted at the paths [volumes] maps them to.
 */
class AdminContainer(
    image: DockerImageName,
    val alias: String,
    network: Network,
    volumes: Map<String, String> = emptyMap(),
) : GenericContainer<AdminContainer>(image) {
    init {
        withNetwork(network)
        withNetworkAliases(alias)
        withCreateContainerCmdModifier {
            it.withEntrypoint("sleep", "infinity")
            it.withHostName(alias)
            it.hostConfig!!.withBinds(volumes.map { (volume, path) -> Bind(volume, Volume(path)) })
        }
    }

    /** Runs `neo4j-admin` with [arguments]. */
    fun admin(vararg arguments: String): ExecResult = execInContainer("neo4j-admin", *arguments)

    /** Like [admin], but fails the test unless the command succeeds; returns what it printed. */
    fun adminOk(vararg arguments: String): String {
        val result = admin(*arguments)
        check(result.exitCode == 0) {
            "$alias: neo4j-admin ${arguments.joinToString(" ")} failed with ${result.exitCode}:\n" +
                result.stdout +
                result.stderr
        }
        return result.stdout
    }

    /** Runs [script] with `sh -c`, failing the test unless it succeeds; returns what it printed. */
    fun shell(script: String): String {
        val result = execInContainer("sh", "-c", script)
        check(result.exitCode == 0) {
            "$alias: $script failed with ${result.exitCode}:\n${result.stdout}${result.stderr}"
        }
        return result.stdout
    }

    /**
     * Configures `neo4j-admin` to take backups over TLS, presenting [certificate] and checking that
     * the server's certificate names the server it dials if [verifyHostname].
     */
    fun backupClient(certificate: TestCertificate, verifyHostname: Boolean = true): AdminContainer {
        certificate.files().forEach { (path, text) ->
            copyFileToContainer(
                Transferable.of(text.toByteArray(), DodecaContainer.READABLE),
                "${DodecaContainer.BACKUP_CERTIFICATES}/$path",
            )
        }
        shell(
            "printf 'dbms.ssl.policy.backup.enabled=true\\n" +
                "dbms.ssl.policy.backup.verify_hostname=$verifyHostname\\n' " +
                ">> /var/lib/neo4j/conf/neo4j.conf"
        )
        return this
    }
}
