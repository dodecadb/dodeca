package dev.dodeca.core.edition

import dev.dodeca.core.Gql
import java.util.stream.Stream
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.api.DatabaseNotFoundException
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Admin
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Name
import org.neo4j.procedure.Procedure

/**
 * `dbms.setDefaultDatabase`, Neo4j Enterprise's procedure that makes another database the default
 * database: for administrators, on `system`, once the default database is stopped. Sessions that
 * name no database, and users without a home database, then use the new one, and the old one is a
 * database like any other, which may be started again or dropped.
 *
 * Procedures only read the system graph in their own transaction: it changes the system graph in a
 * transaction of its own, which with clustering commits only on the leader of `system`.
 */
class DefaultDatabaseProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

    class Result(@JvmField val result: String)

    @Admin
    @SystemProcedure
    @Description(
        "Makes another database the default database. The default database must be stopped first."
    )
    @Procedure(name = "dbms.setDefaultDatabase", mode = Mode.WRITE)
    fun setDefaultDatabase(@Name("databaseName") databaseName: String): Stream<Result> {
        val name = NormalizedDatabaseName(databaseName).name()
        val failed = "Unable to set the default database to '$databaseName'"
        if (name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME) {
            throw refused("$failed: the system database cannot be the default.")
        }
        checkNotNull(graph).beginTx().use { tx ->
            if (change(tx, name, failed)) tx.commit()
        }
        return Stream.of(Result("Default database set to $name"))
    }

    /** Makes [name] the default database in [tx]; false if it is the default already. */
    private fun change(tx: Transaction, name: String, failed: String): Boolean {
        val database = database(tx, name, failed)
        val defaults =
            tx.findNodes(
                    TopologyGraphDbmsModel.DATABASE_LABEL,
                    DefaultDatabase.PROPERTY,
                    true,
                )
                .use { it.asSequence().toList() }
        // So that two changes at once do not leave two default databases.
        defaults.forEach(tx::acquireWriteLock)
        if (
            defaults.any {
                it.getProperty(DefaultDatabase.PROPERTY) != true
            }
        ) {
            return change(tx, name, failed)
        }
        if (database in defaults) return false
        defaults
            .firstOrNull {
                it.getProperty(TopologyGraphDbmsModel.DATABASE_STATUS_PROPERTY, null) !=
                    TopologyGraphDbmsModel.DatabaseStatus.OFFLINE.statusName()
            }
            ?.let { throw refused("$failed: stop the default database '${nameOf(it)}' first.") }
        defaults.forEach { it.setProperty(DefaultDatabase.PROPERTY, false) }
        database.setProperty(DefaultDatabase.PROPERTY, true)
        return true
    }

    /** The `Database` node of the standard database [name]; refuses an alias and a stranger. */
    private fun database(tx: Transaction, name: String, failed: String): Node {
        val names =
            tx.findNodes(
                    TopologyGraphDbmsModel.DATABASE_NAME_LABEL,
                    TopologyGraphDbmsModel.NAME_PROPERTY,
                    name,
                    TopologyGraphDbmsModel.NAMESPACE_PROPERTY,
                    TopologyGraphDbmsModel.DEFAULT_NAMESPACE,
                )
                .use { it.asSequence().toList() }
        val databaseName =
            names.singleOrNull()
                ?: throw DatabaseNotFoundException(
                    GqlHelper.getGql22000_22N51(name),
                    "$failed: Database does not exist.",
                )
        val target =
            databaseName
                .getSingleRelationship(
                    TopologyGraphDbmsModel.TARGETS_RELATIONSHIP,
                    Direction.OUTGOING,
                )
                ?.endNode
        if (databaseName.getProperty(TopologyGraphDbmsModel.PRIMARY_PROPERTY, true) == false) {
            throw refused(
                if (target?.hasLabel(TopologyGraphDbmsModel.DATABASE_LABEL) == true) {
                    "$failed: it is an alias, of '${nameOf(target)}'."
                } else {
                    "$failed: it is a remote alias."
                }
            )
        }
        return target?.takeIf { it.hasLabel(TopologyGraphDbmsModel.DATABASE_LABEL) }
            ?: throw DatabaseNotFoundException(
                GqlHelper.getGql22000_22N51(name),
                "$failed: Database does not exist.",
            )
    }

    private fun nameOf(database: Node): Any? =
        database.getProperty(TopologyGraphDbmsModel.DATABASE_NAME_PROPERTY, null)

    private fun refused(message: String) = Gql.refused(message)
}
