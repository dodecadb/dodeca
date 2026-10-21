package org.dozendb.backup

import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Setting

/**
 * Settings of the server side of backups, named as Neo4j Enterprise's. Registered in
 * `META-INF/services`.
 */
class BackupSettings : SettingsDeclaration {
    companion object {
        const val DEFAULT_PORT = 6362

        @JvmField
        @field:Description("Whether this server serves backups, on server.backup.listen_address.")
        val enabled: Setting<Boolean> =
            newBuilder("server.backup.enabled", BOOL, true).immutable().build()

        @JvmField
        @field:Description(
            "The address this server serves backups on. A port without a host uses " +
                "server.default_listen_address. Backups are served without authentication: on " +
                "an address that is not a loopback one, dbms.ssl.policy.backup.enabled must be " +
                "set, to true for TLS or to false for plaintext."
        )
        val listenAddress: Setting<SocketAddress> =
            newBuilder(
                    "server.backup.listen_address",
                    SOCKET_ADDRESS,
                    SocketAddress("127.0.0.1", DEFAULT_PORT),
                )
                .setDependency(GraphDatabaseSettings.default_listen_address)
                .immutable()
                .build()
    }
}
