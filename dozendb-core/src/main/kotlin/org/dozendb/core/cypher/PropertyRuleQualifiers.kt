package org.dozendb.core.cypher

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.LabelQualifier
import org.neo4j.cypher.internal.ast.`Node$`
import org.neo4j.cypher.internal.ast.PatternQualifier
import org.neo4j.cypher.internal.ast.prettifier.`ExpressionStringifier$`
import org.neo4j.cypher.internal.evaluator.Evaluator
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocationLike
import org.neo4j.cypher.internal.expressions.GreaterThan
import org.neo4j.cypher.internal.expressions.GreaterThanOrEqual
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.IsNull
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.LessThanOrEqual
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.NotEquals
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.procs.ProcedureCallContext
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PropertyRule.ComparisonOperator
import org.neo4j.internal.kernel.api.security.PropertyRule.NullOperator
import org.neo4j.kernel.api.QueryLanguage
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues
import scala.jdk.javaapi.CollectionConverters

/**
 * The property rules of graph privileges, `FOR (n:L) WHERE n.p > 1` and the like, as Neo4j's
 * semantic checks leave them: one property compared with a value, a list or null, perhaps negated,
 * in the kernel's [PatternSegment]s. Rules about relationships, which Neo4j allows only
 * with an internal feature on, and rules about users' attributes, with `abac` functions, are
 * refused.
 */
internal object PropertyRuleQualifiers {
    private const val FAILED = "Failed to administer property rule."

    /**
     * The rule of [qualifier], its value evaluated now with [params] in [tx], so that `date()` is
     * the date of granting. A negation is the opposite operator, as the kernel's operators have it.
     */
    fun rule(
        qualifier: PatternQualifier,
        params: MapValue,
        version: CypherVersion,
        tx: Transaction,
    ): PatternSegment {
        fun evaluate(expression: Expression) = evaluate(expression, params, version, tx)
        if (qualifier.element() !is `Node$`)
            throw unsupported("Property rules on relationships are")
        val abac =
            (listOf(qualifier.expression()) +
                    CollectionConverters.asJava(qualifier.expression().subExpressions()))
                .filterIsInstance<FunctionInvocationLike>()
                .firstOrNull { it.functionName().fullName().startsWith("abac.", ignoreCase = true) }
        if (abac != null) {
            throw unsupported("Property rules with `${abac.functionName().fullName()}()` are")
        }
        val labels =
            CollectionConverters.asJava(qualifier.elementTypeQualifiers())
                .mapNotNull { (it as? LabelQualifier)?.label() }
                .toSet()
        var predicate = qualifier.expression()
        var negated = false
        while (predicate is Not) {
            negated = !negated
            predicate = predicate.rhs()
        }
        fun compare(
            lhs: Expression,
            operator: ComparisonOperator,
            rhs: Expression,
        ): PatternSegment {
            val property = (lhs as Property).propertyKey().name()
            val applied = if (negated) NEGATED.getValue(operator) else operator
            val value = value(evaluate(rhs), applied)
            // Neo4j hands `p <> v` over as `NOT p = v`: the rule is named as written.
            value ?: throw alwaysNull(applied, "$property ${applied.symbol} NULL")
            return PatternSegment.NodeValuePatternSegment(labels, property, value, applied)
        }
        fun isNull(lhs: Expression, operator: NullOperator): PatternSegment =
            PatternSegment.NodeNullPatternSegment(
                labels,
                (lhs as Property).propertyKey().name(),
                if (negated) NEGATED_NULL.getValue(operator) else operator,
            )
        return when (predicate) {
            is Equals -> compare(predicate.lhs(), ComparisonOperator.EQUAL, predicate.rhs())
            is NotEquals -> compare(predicate.lhs(), ComparisonOperator.NOT_EQUAL, predicate.rhs())
            is GreaterThan ->
                compare(predicate.lhs(), ComparisonOperator.GREATER_THAN, predicate.rhs())
            is GreaterThanOrEqual ->
                compare(predicate.lhs(), ComparisonOperator.GREATER_THAN_OR_EQUAL, predicate.rhs())
            is LessThan -> compare(predicate.lhs(), ComparisonOperator.LESS_THAN, predicate.rhs())
            is LessThanOrEqual ->
                compare(predicate.lhs(), ComparisonOperator.LESS_THAN_OR_EQUAL, predicate.rhs())
            is In -> compare(predicate.lhs(), ComparisonOperator.IN, predicate.rhs())
            is IsNull -> isNull(predicate.lhs(), NullOperator.IS_NULL)
            is IsNotNull -> isNull(predicate.lhs(), NullOperator.IS_NOT_NULL)
            is MapExpression -> {
                val item = CollectionConverters.asJava(predicate.items()).single()
                val key = item._1().name()
                val value = item._2()
                val applied =
                    if (negated) ComparisonOperator.NOT_EQUAL else ComparisonOperator.EQUAL
                val evaluated =
                    value(evaluate(value), applied)
                        ?: throw InvalidArgumentException(
                            GqlHelper.getGql22NA0_22NB0("{$key:NULL}"),
                            "$FAILED The property value access rule pattern `{$key:NULL}` " +
                                "always evaluates to `NULL`. Use `WHERE` syntax in combination " +
                                "with `IS NULL` instead.",
                        )
                PatternSegment.NodeValuePatternSegment(labels, key, evaluated, applied)
            }
            else ->
                throw InvalidArgumentException(
                    invalidRule(
                        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA7)
                            .withParam(
                                GqlParams.StringParam.expr,
                                stringify(qualifier.expression()),
                            )
                            .build()
                    ),
                    "$FAILED The expression: `${stringify(qualifier.expression())}` is not " +
                        "supported. Only single, literal-based predicate expressions are allowed " +
                        "for property-based access control.",
                )
        }
    }

    /**
     * That the rule [predicate] with [operator] always evaluates to null, as Neo4j says it of
     * literal nulls, with the same hints.
     */
    private fun alwaysNull(operator: ComparisonOperator, predicate: String) =
        when (operator) {
            ComparisonOperator.EQUAL ->
                InvalidArgumentException(
                    GqlHelper.getGql22NA0_22NA5(predicate),
                    "$FAILED The property value access rule pattern `$predicate` always " +
                        "evaluates to `NULL`. Use `IS NULL` instead.",
                )
            ComparisonOperator.NOT_EQUAL ->
                InvalidArgumentException(
                    GqlHelper.getGql22NA0_22NA6(predicate),
                    "$FAILED The property value access rule pattern `$predicate` always " +
                        "evaluates to `NULL`. Use `IS NOT NULL` instead.",
                )
            else ->
                InvalidArgumentException(
                    GqlHelper.getGql22NA0_22NA4(predicate),
                    "$FAILED The property value access rule pattern `$predicate` always " +
                        "evaluates to `NULL`.",
                )
        }

    /** That [what] (are) not supported, as an invalid property rule. */
    private fun unsupported(what: String) =
        InvalidArgumentException(invalidRule(null), "$FAILED $what not supported.")

    /** Neo4j's GQL status of an invalid property rule, for the reason [cause] if any. */
    private fun invalidRule(cause: ErrorGqlStatusObject?): ErrorGqlStatusObject =
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA0)
            .let { if (cause != null) it.withCause(cause) else it }
            .build()

    /**
     * [evaluated] as a value a property can have, a list for IN as an array; null for null. Values
     * Neo4j's semantic checks cannot see, those of parameters, are refused as it refuses literals.
     */
    private fun value(evaluated: AnyValue, operator: ComparisonOperator): Value? {
        val lists = operator == ComparisonOperator.IN || operator == ComparisonOperator.NOT_IN
        val value =
            when {
                evaluated === Values.NO_VALUE -> return null
                evaluated is ListValue ->
                    try {
                        evaluated.toStorableArray()
                    } catch (_: RuntimeException) {
                        throw InvalidArgumentException(
                            invalidRule(null),
                            "$FAILED All elements in a list must be literals of the same type for " +
                                "property-based access control.",
                        )
                    }
                evaluated is Value && !lists -> evaluated
                else ->
                    throw InvalidArgumentException(
                        invalidRule(null),
                        "$FAILED The value `$evaluated` is not supported for property-based " +
                            "access control${if (lists) ": IN needs a list" else ""}.",
                    )
            }
        if (value is FloatingPointValue && value.isNaN) {
            throw InvalidArgumentException(
                invalidRule(
                    ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22NA3).build()
                ),
                "$FAILED `NaN` is not supported for property-based access control.",
            )
        }
        return value
    }

    /**
     * The value of [expression]: Neo4j's evaluator does not call functions, so those the semantic
     * checks allow, `date()` and the like, are called in [tx] as Cypher calls them, with its clock.
     */
    private fun evaluate(
        expression: Expression,
        params: MapValue,
        version: CypherVersion,
        tx: Transaction,
    ): AnyValue =
        when (expression) {
            is ResolvedFunctionInvocation -> {
                val signature = expression.fcnSignature().get()
                val given =
                    CollectionConverters.asJava(expression.callArguments()).map {
                        evaluate(it, params, version, tx)
                    }
                val defaults =
                    CollectionConverters.asJava(signature.inputSignature()).drop(given.size).map {
                        it.default().get()
                    }
                val procedures = (tx as InternalTransaction).kernelTransaction().procedures()
                val context =
                    ProcedureCallContext(
                        signature.id(),
                        true,
                        GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
                        true,
                        "",
                        EmptyMemoryTracker.INSTANCE,
                        if (version == CypherVersion.Cypher5) QueryLanguage.CYPHER_5
                        else QueryLanguage.CYPHER_25,
                    )
                val arguments = (given + defaults).toTypedArray()
                if (signature.builtIn()) {
                    procedures.builtInFunctionCall(signature.id(), arguments, context)
                } else {
                    procedures.functionCall(signature.id(), arguments, context)
                }
            }
            is ListLiteral ->
                VirtualValues.list(
                    *CollectionConverters.asJava(expression.expressions())
                        .map { evaluate(it, params, version, tx) }
                        .toTypedArray()
                )
            else -> Evaluator.expressionEvaluator(version).evaluate(expression, params)
        }

    private fun stringify(expression: Expression): String =
        `ExpressionStringifier$`.`MODULE$`
            .apply(`ExpressionStringifier$`.`MODULE$`.failingExtender(), false, false, false, false)
            .apply(expression)

    /** The operator that holds when [ComparisonOperator] does not, under three-valued logic. */
    private val NEGATED =
        mapOf(
            ComparisonOperator.EQUAL to ComparisonOperator.NOT_EQUAL,
            ComparisonOperator.NOT_EQUAL to ComparisonOperator.EQUAL,
            ComparisonOperator.GREATER_THAN to ComparisonOperator.LESS_THAN_OR_EQUAL,
            ComparisonOperator.GREATER_THAN_OR_EQUAL to ComparisonOperator.LESS_THAN,
            ComparisonOperator.LESS_THAN to ComparisonOperator.GREATER_THAN_OR_EQUAL,
            ComparisonOperator.LESS_THAN_OR_EQUAL to ComparisonOperator.GREATER_THAN,
            ComparisonOperator.IN to ComparisonOperator.NOT_IN,
            ComparisonOperator.NOT_IN to ComparisonOperator.IN,
        )

    private val NEGATED_NULL =
        mapOf(
            NullOperator.IS_NULL to NullOperator.IS_NOT_NULL,
            NullOperator.IS_NOT_NULL to NullOperator.IS_NULL,
        )
}
