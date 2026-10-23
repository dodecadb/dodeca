package org.dozendb.cluster.internal.forwarding

import org.dozendb.consensus.ratis.TlsConfig
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector

/**
 * Writes that reach a member which does not lead their database, forwarded to the leader's routing
 * connector, where they run as the user they run for (`OPERATOR_GUIDE.md`, "Writes on any member").
 * Only members may log in there as a user, so a member takes forwarded writes only if it can tell
 * members from other clients: by their certificates.
 */
internal object ForwardedWrites {
    /**
     * Why other members may not forward writes to this member, with [config] and the TLS between
     * members [tls], or null if they may.
     */
    fun refusal(config: Config, tls: TlsConfig?): String? =
        if (!config.get(BoltConnector.enabled)) {
            "Bolt, and with it the routing connector, is disabled " +
                "(${BoltConnector.enabled.name()}=false)"
        } else {
            sendingRefusal(config, tls)
        }

    /** Why this member forwards no writes to other members, or null if it does. */
    fun sendingRefusal(config: Config, tls: TlsConfig?): String? =
        when {
            !config.get(GraphDatabaseSettings.routing_enabled) ->
                "server-side routing is disabled " +
                    "(${GraphDatabaseSettings.routing_enabled.name()}=false)"
            tls == null ->
                "members talk in plaintext, so the routing connector cannot tell members from " +
                    "other clients; set dbms.ssl.policy.cluster.enabled=true"
            !tls.requireClientCertificates ->
                "members do not require each other's certificates, so the routing connector " +
                    "cannot tell members from other clients; set " +
                    "dbms.ssl.policy.cluster.client_auth=REQUIRE"
            else -> null
        }
}
