package org.dozendb.core

import java.util.Properties

/** Facts about this DozenDB build. */
object DozenDb {
    private val properties: Properties by lazy {
        Properties().apply {
            DozenDb::class.java.getResourceAsStream("dozendb.properties")?.use(::load)
        }
    }

    /** The DozenDB version of this build. */
    val version: String
        get() = properties.getProperty("version", "unknown")

    /** The Neo4j version this build was compiled and tested against. */
    val builtForNeo4j: String
        get() = properties.getProperty("neo4j.version", "unknown")
}
