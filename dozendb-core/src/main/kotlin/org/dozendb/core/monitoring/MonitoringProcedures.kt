package org.dozendb.core.monitoring

import java.io.IOException
import java.util.stream.Stream
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo
import org.neo4j.kernel.internal.GraphDatabaseAPI
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Procedure

/** Neo4j Enterprise's procedures for looking into a running server, as its manual lists them. */
class MonitoringProcedures {
    @JvmField @Context var graph: GraphDatabaseAPI? = null

    @Description(
        "Starts a checkpoint of the database and waits for it, or for the one under way, to finish."
    )
    @Procedure(name = "db.checkpoint", mode = Mode.DBMS)
    fun checkpoint(): Stream<CheckpointResult> {
        val checkPointer =
            checkNotNull(graph).dependencyResolver.resolveDependency(CheckPointer::class.java)
        val result =
            try {
                checkPointer.tryCheckPoint(SimpleTriggerInfo("Call to db.checkpoint() procedure"))
                CheckpointResult(true, "Checkpoint completed.")
            } catch (e: IOException) {
                CheckpointResult(false, "Checkpoint failed: ${e.message}")
            }
        return Stream.of(result)
    }

    /** A row of `db.checkpoint`. */
    class CheckpointResult(@JvmField val success: Boolean, @JvmField val message: String)
}
