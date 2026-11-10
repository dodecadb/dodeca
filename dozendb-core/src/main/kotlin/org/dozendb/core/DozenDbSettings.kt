package org.dozendb.core

import java.nio.file.Path
import org.neo4j.configuration.Description
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingConstraints.noDuplicates
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.BOOL
import org.neo4j.configuration.SettingValueParsers.INT
import org.neo4j.configuration.SettingValueParsers.PATH
import org.neo4j.configuration.SettingValueParsers.SECURE_STRING
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.configuration.SettingValueParsers.listOf
import org.neo4j.configuration.SettingValueParsers.ofEnum
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.graphdb.config.Setting
import org.neo4j.string.SecureString

/**
 * DozenDB settings that apply with clustering enabled or disabled. Registered in
 * `META-INF/services`. Settings Neo4j Enterprise has too keep Enterprise's names.
 */
class DozenDbSettings : SettingsDeclaration {
    companion object {
        @JvmField
        @field:Description(
            "The maximum number of databases, including system. CREATE DATABASE fails once it " +
                "is reached. Named like Neo4j Enterprise's setting."
        )
        val maxDatabases: Setting<Int> =
            newBuilder("dbms.max_databases", INT, 100).addConstraint(min(2)).dynamic().build()

        @JvmField
        @field:Description(
            "Enables DozenDB clustering. When false, DozenDB runs as a single server."
        )
        val clusterEnabled: Setting<Boolean> =
            newBuilder("dbms.dozendb.cluster.enabled", BOOL, false).immutable().build()

        @JvmField
        @field:Description(
            "The settings dbms.setConfigValue may change while the server runs: setting names, " +
                "comma separated, each of which may have the wildcard '*'. Empty lets none " +
                "change. Named like Neo4j Enterprise's setting."
        )
        val dynamicSettingAllowlist: Setting<List<String>> =
            newBuilder("server.dynamic.setting.allowlist", listOf(STRING), listOf("*")).build()

        @JvmField
        @field:Description(
            "Logs successful logins to the security log. When false, only failed logins are " +
                "logged. Named like Neo4j Enterprise's setting."
        )
        val logSuccessfulAuthentication: Setting<Boolean> =
            newBuilder("dbms.security.log_successful_authentication", BOOL, true).build()

        @JvmField
        @field:Description(
            "The PKCS12 keystore holding the AES key that encrypts the passwords of remote " +
                "database aliases, as an absolute path or relative to the home directory. " +
                "Every member of a cluster needs the same key. Named like Neo4j Enterprise's " +
                "setting."
        )
        val keystorePath: Setting<Path> =
            newBuilder("dbms.security.keystore.path", PATH, null)
                .setDependency(GraphDatabaseSettings.neo4j_home)
                .dynamic()
                .build()

        @JvmField
        @field:Description(
            "The password of the keystore that dbms.security.keystore.path names, which also " +
                "protects the key. Named like Neo4j Enterprise's setting."
        )
        val keystorePassword: Setting<SecureString> =
            newBuilder("dbms.security.keystore.password", SECURE_STRING, null).dynamic().build()

        @JvmField
        @field:Description(
            "The name of the AES key in the keystore that dbms.security.keystore.path names. " +
                "Named like Neo4j Enterprise's setting."
        )
        val keyName: Setting<String> =
            newBuilder("dbms.security.key.name", STRING, null).dynamic().build()

        @JvmField
        @field:Description(
            "Whether the server's process ends, with status 1, once a database panics, so that a " +
                "process manager starts it again; otherwise only the database fails. Named like " +
                "Neo4j Enterprise's setting."
        )
        val shutdownOnPanic: Setting<Boolean> =
            newBuilder("server.panic.shutdown_on_panic", BOOL, false).build()

        @JvmField
        @field:Description(
            "The tags of this server, as SHOW SERVERS shows them, used by " +
                "db.cluster.raft.leader_transfer.priority_tag. They are taken when the server is " +
                "enabled, at the cluster's first start or by ENABLE SERVER; after that, ALTER " +
                "SERVER changes them. Tags have no commas and no duplicates. Named like Neo4j " +
                "Enterprise's setting."
        )
        val initialServerTags: Setting<List<String>> =
            newBuilder("initial.server.tags", listOf(STRING), emptyList())
                .addConstraint(noDuplicates())
                .build()

        @JvmField
        @field:Description(
            "Which copies of databases this server may host: NONE for any, PRIMARY for " +
                "primaries only, SECONDARY for secondaries only. Taken when the server is " +
                "enabled, as initial.server.tags is; after that, ALTER SERVER changes it. Named " +
                "like Neo4j Enterprise's setting."
        )
        val initialServerModeConstraint: Setting<InstanceModeConstraint> =
            newBuilder(
                    "initial.server.mode_constraint",
                    ofEnum(InstanceModeConstraint::class.java),
                    InstanceModeConstraint.NONE,
                )
                .build()

        @JvmField
        @field:Description(
            "The databases this server may host, if not every database; not together with " +
                "initial.server.denied_databases. Taken when the server is enabled, as " +
                "initial.server.tags is. Named like Neo4j Enterprise's setting."
        )
        val initialServerAllowedDatabases: Setting<List<String>> =
            newBuilder("initial.server.allowed_databases", listOf(STRING), emptyList())
                .addConstraint(noDuplicates())
                .build()

        @JvmField
        @field:Description(
            "The databases this server may not host; not together with " +
                "initial.server.allowed_databases. Taken when the server is enabled, as " +
                "initial.server.tags is. Named like Neo4j Enterprise's setting."
        )
        val initialServerDeniedDatabases: Setting<List<String>> =
            newBuilder("initial.server.denied_databases", listOf(STRING), emptyList())
                .addConstraint(noDuplicates())
                .build()
    }
}
