package dev.dodeca.cluster.internal.routing

import dev.dodeca.cluster.MemberMetadata
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.Member
import dev.dodeca.consensus.MemberId
import java.io.ByteArrayOutputStream
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.logging.log4j.Log4jLogProvider

class PolicyAgreementTest {
    private val output = ByteArrayOutputStream()
    private val agreement =
        PolicyAgreement("ours", Log4jLogProvider(output).getLog(PolicyAgreement::class.java))
    private val north = Member(MemberId(UUID(0, 1)), HostPort("north", 7700))
    private val south = Member(MemberId(UUID(0, 2)), HostPort("south", 7700))

    private fun report(digest: String?) =
        if (digest == null) emptyMap() else mapOf(MemberMetadata.ROUTING_POLICIES to digest)

    private fun lines(): List<String> = output.toString().lines().filter { it.isNotBlank() }

    @Test
    fun `a server with other policies is warned about once per change, and its agreeing again told`() {
        agreement.reported(north, report("ours"))
        agreement.reported(south, report(null))
        assertThat(lines()).describedAs("agreeing, or of a version before policies").isEmpty()

        repeat(3) { agreement.reported(north, report("theirs")) }
        agreement.reported(north, report("changed"))
        agreement.reported(north, report("ours"))
        agreement.reported(north, report("ours"))
        agreement.reported(north, report("theirs"))

        assertThat(lines()).hasSize(4)
        assertThat(lines()[0])
            .contains("WARN")
            .contains(
                "Server ${north.id} at north:7700 has other routing policies than this server, " +
                    "so that the routing tables of drivers that name a policy depend on the " +
                    "server they ask: set the same dbms.routing.load_balancing.config." +
                    "server_policies.* on every server"
            )
        assertThat(lines()[1]).contains("WARN").contains("Server ${north.id}")
        assertThat(lines()[2])
            .contains("INFO")
            .contains(
                "Server ${north.id} at north:7700 has the same routing policies as this " +
                    "server again"
            )
        assertThat(lines()[3]).contains("WARN").contains("Server ${north.id}")
    }

    @Test
    fun `servers are told apart`() {
        agreement.reported(north, report("theirs"))
        agreement.reported(south, report("theirs"))

        assertThat(lines()).hasSize(2)
        assertThat(lines()[1]).contains("Server ${south.id} at south:7700")
    }
}
