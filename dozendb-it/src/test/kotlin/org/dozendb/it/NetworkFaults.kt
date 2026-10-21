package org.dozendb.it

import com.github.dockerjava.api.model.Capability
import org.testcontainers.containers.GenericContainer
import org.testcontainers.utility.DockerImageName

/**
 * Network faults of one member: a sidecar container shares the member's network stack and changes
 * it with iptables and tc. Raft traffic can be cut both ways while Bolt keeps working, and what the
 * member sends to another container can be delayed, limited or lost.
 *
 * A restarted member gets a new network stack: close its faults before restarting it and create new
 * ones afterwards.
 */
class NetworkFaults(private val member: GenericContainer<*>) : AutoCloseable {
    private val sidecar: GenericContainer<*> =
        GenericContainer(DockerImageName.parse(IMAGE))
            .withCreateContainerCmdModifier {
                it.hostConfig!!
                    .withNetworkMode("container:${member.containerId}")
                    .withCapAdd(Capability.NET_ADMIN)
            }
            .withCommand("sleep", "infinity")
            .also { it.start() }

    /** Drops every Raft packet to and from the member. */
    fun cutRaft() {
        for (chain in listOf("INPUT", "OUTPUT")) {
            for (port in listOf("--dport", "--sport")) {
                exec("iptables", "-A", chain, "-p", "tcp", port, RAFT_PORT, "-j", "DROP")
            }
        }
    }

    fun healRaft() {
        exec("iptables", "-F")
    }

    /**
     * Applies netem [arguments], such as `rate 10mbit`, to what the member sends to [target]; other
     * traffic is unaffected. Replaces earlier shaping.
     */
    fun shapeTo(target: GenericContainer<*>, vararg arguments: String) {
        clearShaping()
        exec("tc", "qdisc", "add", "dev", "eth0", "root", "handle", "1:", "prio")
        exec(
            "tc",
            "qdisc",
            "add",
            "dev",
            "eth0",
            "parent",
            "1:3",
            "handle",
            "30:",
            "netem",
            *arguments,
        )
        exec(
            "tc",
            "filter",
            "add",
            "dev",
            "eth0",
            "protocol",
            "ip",
            "parent",
            "1:0",
            "prio",
            "3",
            "u32",
            "match",
            "ip",
            "dst",
            "${ipOf(target)}/32",
            "flowid",
            "1:3",
        )
    }

    fun clearShaping() {
        // Fails when nothing is shaped, which is fine.
        sidecar.execInContainer("tc", "qdisc", "del", "dev", "eth0", "root")
    }

    private fun exec(vararg command: String) {
        val result = sidecar.execInContainer(*command)
        check(result.exitCode == 0) { "${command.joinToString(" ")} failed: ${result.stderr}" }
    }

    override fun close() {
        sidecar.stop()
    }

    companion object {
        /** Network tools; pinned so that test runs do not change with the image. */
        const val IMAGE = "nicolaka/netshoot:v0.13"
        private const val RAFT_PORT = "7700"

        fun ipOf(container: GenericContainer<*>): String =
            checkNotNull(
                container.containerInfo.networkSettings.networks.values.first().ipAddress
            ) {
                "${container.networkAliases.firstOrNull() ?: container.containerName} has no IP address"
            }
    }
}
