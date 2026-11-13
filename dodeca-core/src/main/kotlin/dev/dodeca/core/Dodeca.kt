package dev.dodeca.core

import java.util.Properties

/** Facts about this Dodeca build. */
object Dodeca {
    private val properties: Properties by lazy {
        Properties().apply {
            Dodeca::class.java.getResourceAsStream("dodeca.properties")?.use(::load)
        }
    }

    /** The Dodeca version of this build. */
    val version: String
        get() = properties.getProperty("version", "unknown")

    /** The Neo4j version this build was compiled and tested against. */
    val builtForNeo4j: String
        get() = properties.getProperty("neo4j.version", "unknown")
}
