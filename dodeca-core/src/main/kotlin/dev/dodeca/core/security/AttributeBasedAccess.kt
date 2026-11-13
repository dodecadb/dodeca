package dev.dodeca.core.security

import dev.dodeca.core.DodecaSettings
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import org.neo4j.collection.ResourceRawIterator
import org.neo4j.common.DependencyResolver
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.`AdministrationCommand$`
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver
import org.neo4j.cypher.internal.evaluator.StaticEvaluation
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.`ResolvedFunctionInvocation$`
import org.neo4j.cypher.internal.parser.`AstParserFactory$`
import org.neo4j.cypher.internal.runtime.`CypherRow$`
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.`Rewriter$`
import org.neo4j.cypher.internal.util.`UnknownSize$`
import org.neo4j.cypher.internal.util.`bottomUp$`
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.graphdb.security.URLAccessChecker
import org.neo4j.internal.kernel.api.Procedures
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.procs.ProcedureHandle
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserAggregationReducer
import org.neo4j.internal.kernel.api.procs.UserFunctionHandle
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.QueryLanguage
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.kernel.api.procedure.GlobalProcedures
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.logging.InternalLog
import org.neo4j.values.AnyValue
import org.neo4j.values.ValueMapper
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.Function1
import scala.Option
import scala.jdk.javaapi.CollectionConverters

/**
 * Attribute-based access control with the tags of native users, as Neo4j Enterprise 2026.10 has it
 * once `native` is one of [providers]: the roles that auth rules give users whose tags, and the
 * time, fulfil their conditions, and the values that property rules with `abac.native.user_tags()`
 * take for each user. Both are worked out when a transaction starts.
 *
 * Without `native`, auth rules give no roles and those property rules are left out, as Neo4j
 * Enterprise skips what it has no provider for. Dodeca has no OIDC providers: conditions that call
 * `abac.oidc.user_attribute()` never hold, as in Neo4j Enterprise without one.
 */
internal class AttributeBasedAccess(
    private val procedures: GlobalProcedures,
    providers: List<String>,
    private val log: InternalLog,
) {
    private val native = DodecaSettings.NATIVE in providers

    /** The privileges a user has, and the tags they depend on, if any. */
    class Granted(val privileges: List<Privilege>, val tags: List<String>?)

    /**
     * The roles whose privileges [user] has in a transaction that starts at [time], in
     * milliseconds: theirs and PUBLIC, and those of the enabled auth rules whose conditions hold
     * for them; none while they are suspended or unknown.
     */
    fun roles(graph: SecurityGraph, user: String, time: Long): Set<String> {
        val roles = graph.roles(user)
        if (!native || roles.isEmpty()) return roles
        val tags = graph.users[user]?.tags.orEmpty()
        val given =
            graph.authRules
                .filter { rule ->
                    rule.enabled && !roles.containsAll(rule.roles) && holds(graph, rule, tags, time)
                }
                .flatMap { it.roles }
        return roles + given
    }

    /**
     * The privileges of [roles] for [user]: property rules with their tags compare with them, or
     * are left out without `native`.
     */
    fun privileges(graph: SecurityGraph, roles: Set<String>, user: String): Granted {
        val privileges = graph.privileges(roles)
        val tagged = privileges.any { it.segment.isTagged() }
        if (!tagged) return Granted(privileges, null)
        if (!native) {
            val skipped = privileges.filter { it.segment.isTagged() }
            if (graph.firstTime("skipped")) {
                log.warn(
                    "Property rules with abac.native.user_tags() are left out, as " +
                        "${DodecaSettings.abacAuthorizationProviders.name()} does not name " +
                        "${DodecaSettings.NATIVE}: ${skipped.joinToString()}."
                )
            }
            return Granted(privileges - skipped.toSet(), null)
        }
        val tags = graph.users[user]?.tags.orEmpty()
        val list = Values.stringArray(*tags.toTypedArray())
        return Granted(
            privileges.map { privilege ->
                val segment = privilege.segment
                if (!segment.isTagged()) return@map privilege
                val rule = (segment as PrivilegeSegment.PropertyRule).pattern
                privilege.copy(
                    segment =
                        if (segment is PrivilegeSegment.NodePattern) {
                            PrivilegeSegment.NodePattern(
                                PatternSegment.NodeValuePatternSegment(
                                    rule.elementTypes(),
                                    rule.property(),
                                    list,
                                    (rule as PatternSegment.AbacPatternSegment).operator(),
                                )
                            )
                        } else {
                            PrivilegeSegment.RelationshipPattern(
                                PatternSegment.RelValuePatternSegment(
                                    rule.elementTypes(),
                                    rule.property(),
                                    list,
                                    (rule as PatternSegment.AbacPatternSegment).operator(),
                                )
                            )
                        }
                )
            },
            tags,
        )
    }

    private fun PrivilegeSegment.isTagged(): Boolean =
        this is PrivilegeSegment.PropertyRule && pattern is PatternSegment.AbacPatternSegment

    /** A condition as one read of the system graph compiles it. */
    sealed interface Condition {
        /** A condition that cannot hold, with a function Dodeca does not have. */
        data object Never : Condition

        /** A condition whose user tags are [TAGS], and whose functions are resolved. */
        class Evaluated(val expression: Expression) : Condition
    }

    private fun holds(
        graph: SecurityGraph,
        rule: AuthRule,
        tags: List<String>,
        time: Long,
    ): Boolean {
        val condition = graph.conditions.computeIfAbsent(rule) { compile(graph, it) }
        if (condition !is Condition.Evaluated) return false
        return try {
            val params =
                VirtualValues.map(
                    arrayOf(TAGS),
                    arrayOf(VirtualValues.fromList(tags.map { Values.utf8Value(it) })),
                )
            evaluate(condition.expression, params, rule.version, time) == BooleanValue.TRUE
        } catch (e: RuntimeException) {
            if (graph.firstTime("failed ${rule.name}")) {
                log.warn(
                    "The condition of $rule failed, so it gives no roles: ${rule.condition}",
                    e,
                )
            }
            false
        }
    }

    /**
     * [expression] with [params], whose functions run as at [time]: the transaction's start, which
     * the clocks of `date.transaction()` and the like give.
     */
    private fun evaluate(
        expression: Expression,
        params: MapValue,
        version: CypherVersion,
        time: Long,
    ): AnyValue =
        StaticEvaluation.from(
                ClockedProcedures(
                    procedures,
                    Clock.fixed(Instant.ofEpochMilli(time), ZoneOffset.UTC),
                ),
                version,
            )
            .evaluate(expression, params, `CypherRow$`.`MODULE$`.empty())

    /**
     * The condition of [rule], parsed as Neo4j parsed it, with the user's tags for
     * `abac.native.user_tags()` and the functions of procedures resolved, as the planner does.
     */
    private fun compile(graph: SecurityGraph, rule: AuthRule): Condition {
        try {
            val parsed =
                `AstParserFactory$`.`MODULE$`
                    .apply(rule.version)
                    .apply(
                        rule.condition,
                        Neo4jCypherExceptionFactory(rule.condition, Option.empty()),
                        Option.empty(),
                        CollectionConverters.asScala(emptyList<SemanticFeature>()).toSeq(),
                    )
                    .expression()
            val signatures = SignatureResolver.from(procedures.currentView, rule.version)
            val resolved = `ResolvedFunctionInvocation$`.`MODULE$`
            val missing = mutableListOf<String>()
            val rewriter =
                `bottomUp$`.`MODULE$`.apply(
                    `Rewriter$`.`MODULE$`.fromFunction(
                        Function1<Any, Any> { node ->
                            when {
                                node !is FunctionInvocation -> node
                                `AdministrationCommand$`.`MODULE$`.isUserTagsFunction(node) ->
                                    ExplicitParameter(
                                        TAGS,
                                        TAGS_TYPE,
                                        `UnknownSize$`.`MODULE$`,
                                        node.position(),
                                    )
                                node.scopedNeedsToBeResolved(rule.version) -> {
                                    val function =
                                        resolved.fromUnresolved(
                                            Function1 { signatures.functionSignature(it) },
                                            resolved.`fromUnresolved$default$2`(),
                                            resolved.`fromUnresolved$default$3`(),
                                            node,
                                        )
                                    if (function.fcnSignature().isEmpty) {
                                        missing += node.functionName().toString()
                                    }
                                    function.coerceArguments()
                                }
                                else -> node
                            }
                        }
                    ),
                    `bottomUp$`.`MODULE$`.`apply$default$2`(),
                    `bottomUp$`.`MODULE$`.`apply$default$3`(),
                )
            val expression = rewriter.apply(parsed) as Expression
            if (missing.isNotEmpty()) {
                if (graph.firstTime("missing ${rule.name}")) {
                    log.info(
                        "The condition of $rule never holds: Dodeca has no " +
                            "${missing.joinToString()}, for which Neo4j Enterprise needs an OIDC " +
                            "provider."
                    )
                }
                return Condition.Never
            }
            return Condition.Evaluated(expression)
        } catch (e: RuntimeException) {
            log.error("The condition of $rule cannot be read, so it gives no roles.", e)
            return Condition.Never
        }
    }

    /**
     * Neo4j's functions, called with [clock] for their transaction and statement clocks: the only
     * part of a transaction that the functions of conditions use.
     */
    private class ClockedProcedures(
        private val procedures: GlobalProcedures,
        clock: Clock,
    ) : Procedures {
        private val context = ClockContext(clock)

        override fun functionCall(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): AnyValue = procedures.currentView.callFunction(this.context, id, arguments)

        override fun builtInFunctionCall(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): AnyValue = procedures.currentView.callFunction(this.context, id, arguments)

        override fun functionGet(name: QualifiedName, scope: QueryLanguage): UserFunctionHandle =
            procedures.currentView.function(name, scope)

        override fun functionGetAll(
            scope: QueryLanguage
        ): java.util.stream.Stream<UserFunctionSignature> =
            procedures.currentView.getAllNonAggregatingFunctions(scope)

        override fun aggregationFunctionGet(
            name: QualifiedName,
            scope: QueryLanguage,
        ): UserFunctionHandle = unavailable()

        override fun aggregationFunctionGetAll(
            scope: QueryLanguage
        ): java.util.stream.Stream<UserFunctionSignature> = unavailable()

        override fun shadowedNamespaces(scope: QueryLanguage): Set<String> = unavailable()

        override fun procedureGet(name: QualifiedName, scope: QueryLanguage): ProcedureHandle =
            unavailable()

        override fun proceduresGetAll(
            scope: QueryLanguage
        ): java.util.stream.Stream<ProcedureSignature> = unavailable()

        override fun procedureCallRead(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): ResourceRawIterator<
            Array<AnyValue>,
            org.neo4j.internal.kernel.api.exceptions.ProcedureException,
        > = unavailable()

        override fun procedureCallWrite(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): ResourceRawIterator<
            Array<AnyValue>,
            org.neo4j.internal.kernel.api.exceptions.ProcedureException,
        > = unavailable()

        override fun procedureCallSchema(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): ResourceRawIterator<
            Array<AnyValue>,
            org.neo4j.internal.kernel.api.exceptions.ProcedureException,
        > = unavailable()

        override fun procedureCallDbms(
            id: Int,
            arguments: Array<AnyValue>,
            context: ProcedureCallContext,
        ): ResourceRawIterator<
            Array<AnyValue>,
            org.neo4j.internal.kernel.api.exceptions.ProcedureException,
        > = unavailable()

        override fun aggregationFunction(
            id: Int,
            context: ProcedureCallContext,
        ): UserAggregationReducer = unavailable()

        override fun builtInAggregationFunction(
            id: Int,
            context: ProcedureCallContext,
        ): UserAggregationReducer = unavailable()

        override fun signatureVersion(): Long = procedures.currentView.signatureVersion()
    }

    /** The context of a function call that has clocks only, all at the transaction's start. */
    private class ClockContext(private val clock: Clock) : Context {
        override fun valueMapper(): ValueMapper<Any> = unavailable()

        override fun securityContext(): SecurityContext = unavailable()

        override fun dependencyResolver(): DependencyResolver = unavailable()

        override fun graphDatabaseAPI(): GraphDatabaseAPI = unavailable()

        override fun thread(): Thread = Thread.currentThread()

        override fun transaction(): InternalTransaction = unavailable()

        override fun internalTransaction(): InternalTransaction = unavailable()

        override fun internalTransactionOrNull(): InternalTransaction? = null

        override fun kernelTransaction(): KernelTransaction = unavailable()

        override fun systemClock(): Clock = Clock.systemUTC()

        override fun statementClock(): Clock = clock

        override fun transactionClock(): Clock = clock

        override fun urlAccessChecker(): URLAccessChecker = unavailable()

        override fun procedureCallContext(): ProcedureCallContext = ProcedureCallContext.EMPTY

        override fun multiVersioned(): Boolean = false
    }

    private companion object {
        /** The parameter that stands for `abac.native.user_tags()`. */
        const val TAGS = "__dodeca_user_tags"

        val TAGS_TYPE: CypherType =
            org.neo4j.cypher.internal.util.symbols.`package`.CTList(
                org.neo4j.cypher.internal.util.symbols.`package`.CTString()
            )

        fun unavailable(): Nothing =
            throw UnsupportedOperationException("Not available in the conditions of auth rules.")
    }
}
