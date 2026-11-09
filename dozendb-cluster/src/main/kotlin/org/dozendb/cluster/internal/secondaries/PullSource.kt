package org.dozendb.cluster.internal.secondaries

import java.io.OutputStream
import org.dozendb.cluster.internal.secondaries.PullProtocol.Status
import org.dozendb.consensus.BulkSource
import org.dozendb.core.store.CommittedTransactions
import org.neo4j.dbms.database.DatabaseContextProvider
import org.neo4j.kernel.database.Database
import org.neo4j.logging.InternalLog

/**
 * Serves the committed transactions of the databases that run here to the secondaries that pull
 * them ([PullProtocol]), whether this member hosts a primary or a secondary: every copy's
 * transaction log holds the same transactions, with the same append indexes and checksums, and a
 * primary applies only what Raft committed. A secondary further ahead than this member, as one that
 * pulled from another, gets nothing. Checkpoints, and with them the pruning of logs, wait while a
 * pull reads them. [served] is called with a database's name for each pull.
 */
internal class PullSource(
    private val databases: () -> DatabaseContextProvider<*>,
    private val log: InternalLog,
    private val served: (String) -> Unit = {},
) : BulkSource {
    override fun serve(request: ByteArray, out: OutputStream) {
        val pull = PullProtocol.decode(request)
        val name = pull.database.name
        val database = databases().getDatabaseContext(name).orElse(null)?.database() as? Database
        val refusal =
            when {
                database == null -> "there is no database $name here"
                database.namedDatabaseId.databaseId().uuid() != pull.database.id ->
                    "database $name is ${database.namedDatabaseId} here, not ${pull.database.id}"
                !database.isStarted -> "database $name is not running here"
                else -> null
            }
        if (refusal != null) {
            PullProtocol.writeRefusal(out, Status.UNAVAILABLE, refusal)
            return
        }
        served(name)
        CommittedTransactions.read(database!!, pull.after, pull.checksum) { range ->
            when (range) {
                is CommittedTransactions.Pruned ->
                    PullProtocol.writeRefusal(
                        out,
                        Status.PRUNED,
                        "the transaction logs of $name here no longer hold the transactions after " +
                            "append index ${pull.after}",
                    )
                // A secondary may have pulled from a server further ahead: nothing newer here.
                is CommittedTransactions.Behind ->
                    PullProtocol.writeTransactions(
                        out,
                        range.last.appendIndex(),
                        emptySequence(),
                        pull.maximumBytes,
                    )
                is CommittedTransactions.Diverged ->
                    PullProtocol.writeRefusal(
                        out,
                        Status.DIVERGED,
                        "database $name here does not continue the secondary's history: " +
                            range.reason,
                    )
                is CommittedTransactions.Continued -> {
                    val count =
                        PullProtocol.writeTransactions(
                            out,
                            range.last.appendIndex(),
                            range.transactions,
                            pull.maximumBytes,
                        )
                    if (count > 0) {
                        log.debug(
                            "Served %d transactions of %s after append index %d",
                            count,
                            name,
                            pull.after,
                        )
                    }
                }
            }
        }
    }
}
