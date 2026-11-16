/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbCypherEngineProvider, now a subclass of
 * Neo4j's CommunityCypherEngineProvider instead of a copy of it.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.cypher

import org.neo4j.cypher.internal.CompilerFactory
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.javacompat.CommunityCypherEngineProvider
import org.neo4j.cypher.internal.javacompat.GraphDatabaseCypherService
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.kernel.impl.query.QueryEngineProvider
import org.neo4j.kernel.internal.GraphDatabaseAPI

/** Neo4j Community's Cypher engine, with Dodeca's administration commands on `system`. */
class DodecaCypherEngineProvider : CommunityCypherEngineProvider() {
    override fun makeCompilerFactory(
        queryService: GraphDatabaseCypherService,
        spi: QueryEngineProvider.SPI,
        parsingConfig: CypherParsingConfig,
        plannerConfig: CypherPlannerConfiguration,
        runtimeConfig: CypherRuntimeConfiguration,
        queryCaches: CypherQueryCaches,
        graphDatabaseAPI: GraphDatabaseAPI,
    ): CompilerFactory =
        DodecaCompilerFactory(
            queryService,
            spi.monitors(),
            spi.logProvider(),
            parsingConfig,
            plannerConfig,
            runtimeConfig,
            queryCaches,
        )
}
