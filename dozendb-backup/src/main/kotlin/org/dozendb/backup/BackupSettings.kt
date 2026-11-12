package org.dozendb.backup

import org.dozendb.backup.seed.SeedProvider
import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingConstraint
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.SOCKET_ADDRESS
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.graphdb.config.Configuration
import org.neo4j.graphdb.config.Setting

/**
 * Settings of the server side of backups and of seeds, named as Neo4j Enterprise's. Registered in
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

        @JvmField
        @field:Description(
            "The providers of the seeds that CREATE DATABASE ... OPTIONS {seedURI: ...} reads, " +
                "the first one for a URI's scheme first: FileSeedProvider for file: URIs, " +
                "CloudSeedProvider for the s3:, gs: and azb: URIs of cloud storage, " +
                "S3SeedProvider, Neo4j Enterprise's deprecated name for it, for s3: URIs, " +
                "URLConnectionSeedProvider for http:, https: and ftp: URIs, and " +
                "ServerSeedProvider for server: URIs, which name a server and a seed in its " +
                "server.directories.seeds. Empty disables seeds."
        )
        val seedProviders: Setting<List<String>> =
            newBuilder(
                    "dbms.databases.seed_from_uri_providers",
                    listOf(STRING),
                    listOf(
                        SeedProvider.File.name,
                        SeedProvider.Cloud.name,
                        SeedProvider.Server.name,
                    ),
                )
                .addConstraint(
                    object : SettingConstraint<List<String>>() {
                        override fun validate(value: List<String>, config: Configuration?) {
                            val unknown = value.filter { SeedProvider.named(it) == null }
                            require(unknown.isEmpty()) {
                                "DozenDB has no seed provider ${unknown.joinToString()}: it has " +
                                    SeedProvider.ALL.joinToString { it.name }
                            }
                        }

                        override fun getDescription() =
                            "is a list of ${SeedProvider.ALL.joinToString { it.name }}"
                    }
                )
                .build()
    }
}
