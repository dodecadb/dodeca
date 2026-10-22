package org.dozendb.core.cypher

import java.time.Clock
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.options.CypherInterpretedPipesFallbackOption
import org.neo4j.cypher.internal.options.CypherOperatorEngineOption
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.SchemaRead
import org.neo4j.kernel.api.AssertOpen
import org.neo4j.logging.InternalLog

/**
 * Neo4j Community's runtime contexts, except that they are not Community's: SHOW PROCEDURES and
 * SHOW FUNCTIONS then fill their `roles` columns and take EXECUTABLE BY, which Neo4j works out from
 * SHOW PRIVILEGES and SHOW USERS. Nothing else in Neo4j 5.26 reads `isCommunity`.
 */
internal class DozenDbRuntimeContextManager(
    private val log: InternalLog,
    private val config: CypherRuntimeConfiguration,
) : CommunityRuntimeContextManager(log, config) {
    override fun create(
        cypherVersion: CypherVersion,
        tokenContext: ReadTokenContext,
        schemaRead: SchemaRead,
        procedures: Procedures,
        clock: Clock,
        debugOptions: CypherDebugOptions,
        compileExpressions: Boolean,
        materializedEntitiesMode: Boolean,
        operatorEngine: CypherOperatorEngineOption,
        interpretedPipesFallback: CypherInterpretedPipesFallbackOption,
        anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
        assertOpen: AssertOpen,
    ): CommunityRuntimeContext =
        object :
            CommunityRuntimeContext(
                cypherVersion,
                tokenContext,
                schemaRead,
                procedures,
                log,
                config,
                anonymousVariableNameGenerator,
                assertOpen,
            ) {
            override fun isCommunity(): Boolean = false
        }
}
