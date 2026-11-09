package org.dozendb.cluster.internal.secondaries

import org.dozendb.core.edition.HostedDatabase
import org.neo4j.dbms.database.readonly.DatabaseReadOnlyChecker
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.HostedOnMode
import org.neo4j.kernel.database.Database
import org.neo4j.kernel.impl.api.CommandCommitListeners
import org.neo4j.kernel.impl.api.LeaseService
import org.neo4j.kernel.impl.api.TransactionCommitProcess
import org.neo4j.kernel.impl.api.TransactionalProcessFactory
import org.neo4j.kernel.impl.transaction.log.TransactionAppender
import org.neo4j.storageengine.api.StorageEngine

/**
 * A database hosted as [initial] until [switchTo] hosts it otherwise, as `system` is when a server
 * turns from a primary of it into a secondary or back: Neo4j and its components keep the database
 * they hold, and take its leases and commit process from the current hosting each time it starts.
 * Switch only while the database is stopped. Neo4j reads the mode it reports once, which stays
 * [initial]'s; it tells only that the database is clustered.
 */
internal class SwitchingHostedDatabase(initial: HostedDatabase) : HostedDatabase {
    @Volatile
    var current: HostedDatabase = initial
        private set

    private lateinit var database: Database

    override val hostedOnMode: HostedOnMode = initial.hostedOnMode

    override val leaseService = LeaseService { current.leaseService.newClient() }

    override fun commitProcessFactory(
        local: TransactionalProcessFactory
    ): TransactionalProcessFactory =
        object : TransactionalProcessFactory by local {
            override fun create(
                appender: TransactionAppender,
                storageEngine: StorageEngine,
                readOnlyDatabaseChecker: DatabaseReadOnlyChecker,
                preAllocateSpaceInStoreFiles: Boolean,
                commandCommitListeners: CommandCommitListeners,
                prefetchPages: Boolean,
            ): TransactionCommitProcess =
                current
                    .commitProcessFactory(local)
                    .create(
                        appender,
                        storageEngine,
                        readOnlyDatabaseChecker,
                        preAllocateSpaceInStoreFiles,
                        commandCommitListeners,
                        prefetchPages,
                    )
        }

    override fun attach(database: Database) {
        this.database = database
        current.attach(database)
    }

    /** Hosts the database, which must be stopped, as [next] from when it next starts. */
    fun switchTo(next: HostedDatabase) {
        next.attach(database)
        current = next
    }
}
