/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozerDB
 * https://dozerdb.org
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-10-19: ported to Kotlin from DozerDB's DozerDbCompilerFactory (Scala).
 * 2026-11-11: SHOW CURRENT USER through DodecaQueryRuntime.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.cypher

import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CommunityRuntimeFactory
import org.neo4j.cypher.internal.`CommunitySchemaCommandRuntime$`
import org.neo4j.cypher.internal.Compiler
import org.neo4j.cypher.internal.CompilerFactory
import org.neo4j.cypher.internal.CypherCurrentCompiler
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.MasterCompiler
import org.neo4j.cypher.internal.SchemaCommandRuntime
import org.neo4j.cypher.internal.cache.CypherQueryCaches
import org.neo4j.cypher.internal.compiler.CypherParsingConfig
import org.neo4j.cypher.internal.compiler.CypherPlannerConfiguration
import org.neo4j.cypher.internal.frontend.notification.InternalNotificationStats
import org.neo4j.cypher.internal.frontend.phases.InternalUsageStats
import org.neo4j.cypher.internal.options.CypherPlannerOption
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.planning.DefaultCypherPlanner
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.logging.InternalLogProvider
import org.neo4j.monitoring.Monitors
import scala.Function0
import scala.Option

/**
 * Neo4j Community's compiler factory, with [DodecaAdministrationCommandRuntime] for the
 * administration commands that run on `system`, and [DodecaQueryRuntime] for the other queries.
 */
internal class DodecaCompilerFactory(
    private val graph: GraphDatabaseQueryService,
    private val kernelMonitors: Monitors,
    logProvider: InternalLogProvider,
    private val parsingConfig: CypherParsingConfig,
    private val plannerConfig: CypherPlannerConfiguration,
    private val runtimeConfig: CypherRuntimeConfiguration,
    private val queryCaches: CypherQueryCaches,
) : CompilerFactory {
    private val log = logProvider.getLog(javaClass)

    override fun supportsAdministrativeCommands(): Boolean = plannerConfig.planSystemCommands()

    override fun createCompiler(
        cypherPlanner: CypherPlannerOption,
        cypherRuntime: CypherRuntimeOption,
        materializedEntitiesMode: Boolean,
        executionEngineProvider: Function0<ExecutionEngine>,
        outerExecutionEngineProvider: Option<Function0<ExecutionEngine>>,
    ): Compiler {
        val dependencies = graph.dependencyResolver
        val planner =
            DefaultCypherPlanner.apply(
                parsingConfig,
                plannerConfig,
                MasterCompiler.CLOCK(),
                kernelMonitors,
                log,
                dependencies.resolveDependency(AbstractSecurityLog::class.java),
                queryCaches,
                cypherPlanner,
                dependencies.resolveDependency(DatabaseReferenceRepository::class.java),
                SCHEMA_COMMANDS,
                dependencies.resolveDependency(InternalNotificationStats::class.java),
                dependencies.resolveDependency(InternalUsageStats::class.java),
            )
        val runtime =
            if (plannerConfig.planSystemCommands()) {
                DodecaAdministrationCommandRuntime(executionEngineProvider.apply(), dependencies)
            } else {
                DodecaQueryRuntime(
                    CommunityRuntimeFactory.getRuntime(
                        cypherRuntime,
                        plannerConfig.useErrorsOverWarnings().apply() as Boolean,
                    )
                )
            }
        // Runtimes accept any context their context type is a supertype of; Scala's variance
        // annotation on CypherRuntime does not reach Kotlin.
        @Suppress("UNCHECKED_CAST")
        return CypherCurrentCompiler(
            planner,
            runtime as CypherRuntime<CommunityRuntimeContext>,
            DodecaRuntimeContextManager(log, runtimeConfig),
            SCHEMA_COMMANDS,
            kernelMonitors,
            queryCaches,
        )
    }

    private companion object {
        /** Neo4j Community's, which plans every kind of constraint the kernel may refuse. */
        val SCHEMA_COMMANDS: SchemaCommandRuntime = `CommunitySchemaCommandRuntime$`.`MODULE$`
    }
}
