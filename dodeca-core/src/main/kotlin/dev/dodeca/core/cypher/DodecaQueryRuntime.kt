/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) The Dodeca Authors
 * 2026-11-11: Neo4j 2026's ShowCurrentUserCommunityCommand, ported to Kotlin, which shows the
 * roles, status and home database of the current user where Community shows null; run through
 * the fallback pipe mapper of Neo4j's interpreted runtime.
 * 2026-11-12: tags only with SHOW USER METADATA.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package dev.dodeca.core.cypher

import dev.dodeca.core.security.BuiltInRoles
import dev.dodeca.core.security.PrivilegeRecords
import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.InterpretedRuntime
import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.ast.ShowCurrentUserClause
import org.neo4j.cypher.internal.logical.plans.CommandDefaultColumn
import org.neo4j.cypher.internal.logical.plans.CommandYieldColumn
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ShowCurrentUser
import org.neo4j.cypher.internal.options.CypherRuntimeOption
import org.neo4j.cypher.internal.runtime.ClosingIterator
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.interpreted.commands.showcommands.Command
import org.neo4j.cypher.internal.runtime.interpreted.pipes.CommandPipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.Pipe
import org.neo4j.cypher.internal.runtime.interpreted.pipes.PipeMapper
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_CREDENTIALS_EXPIRED_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_HOME_DB_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_LABEL
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_NAME_PROPERTY
import org.neo4j.dbms.systemgraph.SecurityGraphDbmsModel.USER_SUSPENDED_PROPERTY
import org.neo4j.exceptions.InternalException
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.NotFoundException
import org.neo4j.kernel.impl.query.TransactionalContext.DatabaseMode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.VirtualValues
import scala.Option
import scala.collection.immutable.List as ScalaList
import scala.collection.immutable.Map as ScalaMap
import scala.collection.immutable.`Map$`
import scala.jdk.javaapi.CollectionConverters

/**
 * The runtime of the queries that are not administration commands: Neo4j Community's [community],
 * but for SHOW CURRENT USER, which Neo4j 2026 runs as a query, as it does SHOW PROCEDURES, and
 * which Community answers with null roles, status and home database. Plans that show the current
 * user run in Neo4j's interpreted runtime, with [CurrentUserCommand] for its rows and Neo4j's own
 * operators for the YIELD, WHERE and RETURN around it.
 */
internal class DodecaQueryRuntime(private val community: CypherRuntime<RuntimeContext>) :
    CypherRuntime<RuntimeContext> {
    override fun name(): String = community.name()

    override fun correspondingRuntimeOption(): Option<CypherRuntimeOption> =
        community.correspondingRuntimeOption()

    override fun compileToExecutable(
        logicalQuery: LogicalQuery,
        context: RuntimeContext,
        databaseMode: DatabaseMode,
    ): ExecutionPlan =
        if (showsCurrentUser(logicalQuery.logicalPlan())) {
            CurrentUserRuntime.compileToExecutable(logicalQuery, context, databaseMode)
        } else {
            community.compileToExecutable(logicalQuery, context, databaseMode)
        }

    private fun showsCurrentUser(plan: LogicalPlan): Boolean =
        plan is ShowCurrentUser ||
            listOf(plan.lhs(), plan.rhs()).any { it.isDefined && showsCurrentUser(it.get()) }

    /** Neo4j's interpreted runtime, with Neo4j's fallback for Enterprise's operators wrapped. */
    private object CurrentUserRuntime : InterpretedRuntime<RuntimeContext> {
        override fun getFallbackPipeMapper(initialFallback: PipeMapper): PipeMapper =
            CurrentUserPipes(initialFallback)
    }

    /** Neo4j's [pipes], with [CurrentUserCommand] for SHOW CURRENT USER. */
    private class CurrentUserPipes(private val pipes: PipeMapper) : PipeMapper by pipes {
        override fun onLeaf(plan: LogicalPlan): Pipe =
            if (plan is ShowCurrentUser) {
                CommandPipe(
                    CurrentUserCommand(plan.defaultColumns(), plan.yieldColumns()),
                    plan.id(),
                )
            } else {
                pipes.onLeaf(plan)
            }
    }
}

/**
 * The row of SHOW CURRENT USER, as SHOW USERS has it: the roles granted to the user by name, then
 * PUBLIC, the status and the home database. None with authentication disabled, as there is no
 * current user then, nor once the user was dropped.
 */
private class CurrentUserCommand(
    defaultColumns: ScalaList<CommandDefaultColumn>,
    yieldColumns: ScalaList<CommandYieldColumn>,
) : Command(defaultColumns, yieldColumns) {
    override fun originalNameRows(
        state: QueryState,
        baseRow: CypherRow,
    ): ClosingIterator<ScalaMap<String, AnyValue>> {
        val query = state.query()
        mustRunOnSystemCheck(query, "SHOW CURRENT USER")
        val securityContext = query.transactionalContext().securityContext()
        val name = securityContext.subject().executingUser()
        val tags = UserTags.visible(securityContext)
        val columns = CollectionConverters.asJava(requestedColumnsNames())
        // The transaction of the query may not read the system graph; Neo4j's command opens its
        // own.
        val row =
            if (name.isEmpty()) {
                null
            } else {
                query.systemGraph().beginTx().use { tx ->
                    try {
                        tx.findNode(USER_LABEL, USER_NAME_PROPERTY, name)?.let { user ->
                            columns.associateWith { column -> value(user, name, column, tags) }
                        }
                    } catch (_: NotFoundException) {
                        null // Dropped while it was read.
                    }
                }
            }
        return if (row == null) {
            ClosingIterator.apply(java.util.Collections.emptyIterator())
        } else {
            ClosingIterator.single(`Map$`.`MODULE$`.from(CollectionConverters.asScala(row)))
        }
    }

    /** The [column] of [user], [name], whose tags are shown if [tags]. */
    private fun value(user: Node, name: String, column: String, tags: Boolean): AnyValue =
        when (column) {
            ShowCurrentUserClause.userColumn() -> Values.utf8Value(name)
            ShowCurrentUserClause.rolesColumn() -> strings(roles(user))
            ShowCurrentUserClause.pwdChangeReqColumn() ->
                Values.of(user.getProperty(USER_CREDENTIALS_EXPIRED_PROPERTY, null))
            ShowCurrentUserClause.suspendedColumn() ->
                Values.booleanValue(user.getProperty(USER_SUSPENDED_PROPERTY, false) as Boolean)
            ShowCurrentUserClause.homeColumn() ->
                Values.of(user.getProperty(USER_HOME_DB_PROPERTY, null))
            ShowCurrentUserClause.tagsColumn() ->
                if (tags) strings(UserTags.read(user)) else Values.NO_VALUE
            else ->
                throw InternalException.internalError(
                    javaClass.simpleName,
                    "Unknown column for show current user. Missing case for column: $column.",
                    "Missing case for column: $column",
                )
        }

    private fun roles(user: Node): List<String> =
        user.getRelationships(Direction.OUTGOING, PrivilegeRecords.HAS_ROLE).use { granted ->
            granted
                .map { it.endNode }
                .filter { it.hasLabel(PrivilegeRecords.ROLE) }
                .map { it.getProperty(PrivilegeRecords.ROLE_NAME) as String }
                .sorted()
        } + BuiltInRoles.PUBLIC

    private fun strings(values: List<String>): AnyValue =
        VirtualValues.fromList(values.map { Values.utf8Value(it) })
}
