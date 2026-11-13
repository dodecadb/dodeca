package dev.dodeca.core.cypher

import java.time.Clock
import org.neo4j.cypher.internal.CommunityRuntimeContext
import org.neo4j.cypher.internal.CommunityRuntimeContextManager
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.compiler.ExecutionModel
import org.neo4j.cypher.internal.options.CypherDebugOptions
import org.neo4j.cypher.internal.planner.spi.IndexComparatorFactory
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.preparser.QueryOptions
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.kernel.impl.query.TransactionalContext
import org.neo4j.logging.InternalLog

/**
 * Neo4j Community's runtime contexts, except that they are not Community's: SHOW PROCEDURES and
 * SHOW FUNCTIONS then fill their `roles` columns and take EXECUTABLE BY, which Neo4j works out from
 * SHOW PRIVILEGES and SHOW USERS. Nothing else in Neo4j reads `isCommunity`.
 */
internal class DodecaRuntimeContextManager(
    log: InternalLog,
    config: CypherRuntimeConfiguration,
) : CommunityRuntimeContextManager(log, config) {
    override fun create(
        cypherVersion: CypherVersion,
        tokenContext: ReadTokenContext,
        transactionalContext: TransactionalContext,
        clock: Clock,
        debugOptions: CypherDebugOptions,
        queryOptions: QueryOptions,
        anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
        executionModel: ExecutionModel,
        indexComparatorFactory: IndexComparatorFactory,
    ): CommunityRuntimeContext {
        val community =
            super.create(
                cypherVersion,
                tokenContext,
                transactionalContext,
                clock,
                debugOptions,
                queryOptions,
                anonymousVariableNameGenerator,
                executionModel,
                indexComparatorFactory,
            )
        return object :
            CommunityRuntimeContext(
                community.cypherVersion(),
                community.tokenContext(),
                community.schemaRead(),
                community.procedures(),
                community.log(),
                community.config(),
                community.anonymousVariableNameGenerator(),
                community.assertOpen(),
                community.indexComparatorFactory(),
            ) {
            override fun isCommunity(): Boolean = false
        }
    }
}
