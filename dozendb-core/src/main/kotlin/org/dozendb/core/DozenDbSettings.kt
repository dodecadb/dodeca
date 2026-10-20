package org.dozendb.core

import org.neo4j.configuration.Description
import org.neo4j.configuration.SettingConstraints.min
import org.neo4j.configuration.SettingImpl.newBuilder
import org.neo4j.configuration.SettingValueParsers.INT
import org.neo4j.configuration.SettingsDeclaration
import org.neo4j.graphdb.config.Setting

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
    }
}
