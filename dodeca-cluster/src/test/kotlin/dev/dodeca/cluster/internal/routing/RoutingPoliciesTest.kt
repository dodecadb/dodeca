package dev.dodeca.cluster.internal.routing

import dev.dodeca.cluster.internal.ClusterSettings
import dev.dodeca.core.servers.ServerRules
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.routing.RoutingException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder

class RoutingPoliciesTest {
    private fun config(vararg settings: Pair<String, String>): Config =
        Config.newBuilder()
            .set(GraphDatabaseSettings.neo4j_home, Path.of("/neo4j"))
            .setRaw(settings.toMap())
            .build()

    private fun policy(name: String) = "${ServerPolicy.PREFIX}.$name"

    private fun context(policy: String? = null): MapValue {
        val builder = MapValueBuilder()
        builder.add("address", Values.stringValue("server1:7687"))
        if (policy != null) builder.add(RoutingPolicies.CONTEXT_KEY, Values.stringValue(policy))
        return builder.build()
    }

    @Test
    fun `the configured policies, and default, all() unless set, for drivers that name none`() {
        val policies =
            RoutingPolicies.of(
                config(
                    policy("north1_only") to "tags(north1)->min(2); halt();",
                    // As neo4j.conf reads lines continued with a backslash.
                    policy("north_app1") to "tags(north1,north2)->min(2);tags(north);all();",
                )
            )

        assertThat(policies.policies.keys)
            .containsExactlyInAnyOrder(
                "default",
                "north1_only",
                "north_app1",
            )
        assertThat(policies.of(context())).isEqualTo(ServerRules.ALL)
        assertThat(policies.of(context("north1_only")))
            .isEqualTo(ServerRules.parse("tags(north1) -> min(2); halt()"))
        val configured = RoutingPolicies.of(config(policy("default") to "tags(north)"))
        assertThat(configured.of(context())).isEqualTo(ServerRules.parse("tags(north)"))
    }

    @Test
    fun `a policy that is not defined fails the request as Neo4j has it, naming the policies`() {
        val policies = RoutingPolicies.of(config(policy("north1") to "tags(north1)->min(2)"))

        for (name in listOf("south", "North1")) {
            assertThatThrownBy { policies.of(context(name)) }
                .isInstanceOfSatisfying(RoutingException::class.java) {
                    assertThat(it.status()).isEqualTo(Status.Routing.RoutingFailed)
                    assertThat(it.gqlStatusObject().gqlStatus()).isEqualTo("08N15")
                }
                .hasMessage(
                    "Policy definition for '$name' could not be found. The routing policies " +
                        "of this server are default, north1."
                )
        }
    }

    @Test
    fun `a policy whose name or rules are unclear fails the configuration`() {
        assertThatThrownBy { config(policy("north-1") to "tags(north1)") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(
                "the routing policy name 'north-1' has other characters than letters, digits " +
                    "and underscores"
            )
        assertThatThrownBy { config(policy("north1") to "tags(north1)->min(2), halt()") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining(policy("north1"))
            .hasMessageContaining("a comma separates rules")
    }

    @Test
    fun `server_policies is the only load balancing plugin, and tables are shuffled by default`() {
        assertThat(config().get(ClusterSettings.loadBalancingPlugin)).isEqualTo("server_policies")
        assertThat(config().get(ClusterSettings.loadBalancingShuffle)).isTrue()
        assertThatThrownBy { config("dbms.routing.load_balancing.plugin" to "nearest") }
            .isInstanceOf(IllegalArgumentException::class.java)
            .hasMessageContaining("the load balancing plugin nearest does not exist")
    }

    @Test
    fun `servers whose policies choose alike have the same digest, however they spell them`() {
        val digest =
            RoutingPolicies.of(config(policy("north1") to "groups(north1)->min(2);halt();")).digest

        assertThat(digest).hasSize(16)
        assertThat(
                RoutingPolicies.of(config(policy("north1") to " tags(north1) -> min(2); halt()"))
                    .digest
            )
            .isEqualTo(digest)
        assertThat(RoutingPolicies.of(config(policy("north1") to "tags(north1) -> min(2)")).digest)
            .isNotEqualTo(digest)
        assertThat(
                RoutingPolicies.of(config(policy("north2") to "tags(north1) -> min(2); halt()"))
                    .digest
            )
            .describedAs("another name")
            .isNotEqualTo(digest)
        assertThat(RoutingPolicies.of(config(policy("default") to "all()")).digest)
            .describedAs("default set as it is unset")
            .isEqualTo(RoutingPolicies.of(config()).digest)
    }
}
