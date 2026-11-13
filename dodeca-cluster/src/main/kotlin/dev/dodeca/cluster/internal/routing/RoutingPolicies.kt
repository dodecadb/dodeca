package dev.dodeca.cluster.internal.routing

import dev.dodeca.core.servers.ServerRules
import java.security.MessageDigest
import java.util.HexFormat
import org.neo4j.configuration.Config
import org.neo4j.configuration.Description
import org.neo4j.configuration.GroupSetting
import org.neo4j.configuration.GroupSettingHelper
import org.neo4j.configuration.SettingConstraint
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.dbms.routing.RoutingException
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.config.Setting
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.virtual.MapValue

/**
 * `dbms.routing.load_balancing.config.server_policies.<name>`: a routing policy, as in Neo4j
 * Enterprise, written in [ServerRules]. A Neo4j group setting, registered in `META-INF/services`.
 * Neo4j takes the name up to the next dot; one with other characters than letters, digits and
 * underscores is refused, as are rules that do not parse.
 */
class ServerPolicy private constructor(private val policy: String?) : GroupSetting {
    /** The instance without a policy, on which Neo4j finds the prefix. */
    constructor() : this(null)

    @JvmField
    @field:Description(
        "A routing policy, which drivers select by its name: rules separated by ';', asked in " +
            "order, the first that passes a server wins. A rule chains filters with '->', from " +
            "every server: tags(a, b) passes those with any of the tags, min(n) passes the " +
            "servers if there are at least n, all() every server, and halt(), which only ends " +
            "the last rule, none, and no all() follows the rules. Drivers that name no policy " +
            "use the one named default, all() unless set. Every server must have the same " +
            "policies. Named like Neo4j Enterprise's setting."
    )
    val rules: Setting<String>? = policy?.let { name ->
        GroupSettingHelper.getBuilder(PREFIX, name, STRING, null)
            .addConstraint(
                object : SettingConstraint<String>() {
                    override fun validate(value: String?, config: Configuration) {
                        require(NAME.matches(name)) {
                            "the routing policy name '$name' has other characters than " +
                                "letters, digits and underscores"
                        }
                        if (value != null) ServerRules.parse(value)
                    }

                    override fun getDescription() =
                        "A name of letters, digits and underscores, and rules that parse."
                }
            )
            .immutable()
            .build()
    }

    override fun name(): String? = policy

    override fun getPrefix(): String = PREFIX

    companion object {
        const val PREFIX = "dbms.routing.load_balancing.config.server_policies"

        private val NAME = Regex("[A-Za-z0-9_]+")

        /** The setting of the policy [name]. */
        fun of(name: String): Setting<String> = checkNotNull(ServerPolicy(name).rules)
    }
}

/**
 * The routing policies of this server, by their case-sensitive names: those its configuration sets,
 * and [DEFAULT], `all()` unless set, which drivers that name none use.
 */
class RoutingPolicies(policies: Map<String, ServerRules>) {
    val policies: Map<String, ServerRules> = (mapOf(DEFAULT to ServerRules.ALL) + policies)

    /**
     * A digest of [policies], the same on servers whose policies have the same names and choose
     * alike, however their rules are spelt.
     */
    val digest: String =
        HexFormat.of()
            .formatHex(
                MessageDigest.getInstance("SHA-256")
                    .digest(
                        this.policies
                            .toSortedMap()
                            .entries
                            .joinToString("\n") { (name, rules) -> "$name=$rules" }
                            .toByteArray()
                    )
            )
            .take(DIGEST_LENGTH)

    /**
     * The policy a driver names in its [routingContext], under [CONTEXT_KEY], or [DEFAULT] if it
     * names none. A name that is not a policy here fails the request, with the status and the
     * message Neo4j has for it.
     */
    fun of(routingContext: MapValue): ServerRules {
        val name = name(routingContext.get(CONTEXT_KEY))
        return policies[name]
            ?: throw RoutingException.policyDefinitionNotFound(name).let {
                RoutingException(
                    it.gqlStatusObject(),
                    it.status(),
                    "${it.message} The routing policies of this server are " +
                        "${policies.keys.sorted().joinToString()}.",
                )
            }
    }

    private fun name(value: AnyValue): String =
        when (value) {
            is NoValue -> DEFAULT
            is TextValue -> value.stringValue()
            else -> value.toString()
        }

    companion object {
        /** The policy of drivers that name none. */
        const val DEFAULT = "default"

        /** The key of the policy name in a driver's routing context. */
        const val CONTEXT_KEY = "policy"

        private const val DIGEST_LENGTH = 16

        /** The policies [config] sets. */
        fun of(config: Config): RoutingPolicies =
            RoutingPolicies(
                config
                    .getGroups(ServerPolicy::class.java)
                    .mapNotNull { (name, group) ->
                        config.get(checkNotNull(group.rules))?.let { name to ServerRules.parse(it) }
                    }
                    .toMap()
            )
    }
}
