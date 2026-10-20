package org.dozendb.cluster.internal

import java.util.UUID
import org.dozendb.cluster.internal.edition.DatabaseManager
import org.dozendb.cluster.internal.replication.LocalDatabase
import org.dozendb.cluster.internal.replication.LocalDatabases
import org.dozendb.cluster.internal.replication.ReplicationService
import org.dozendb.cluster.internal.storecopy.Neo4jLocalStores
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.HostPort
import org.neo4j.dbms.database.DatabaseRepository
import org.neo4j.dbms.database.StandaloneDatabaseContext
import org.neo4j.kernel.database.NamedDatabaseId

/** This member's databases, through the database manager, their files and replication. */
internal class Neo4jLocalDatabases(
    private val manager: DatabaseManager,
    private val repository: DatabaseRepository<StandaloneDatabaseContext>,
    private val stores: Neo4jLocalStores,
    private val replication: ReplicationService,
) : LocalDatabases {
    override fun registered(): Map<NamedDatabaseId, LocalDatabase> =
        repository.registeredDatabases().mapValues { (_, context) ->
            LocalDatabase(started = context.database().isStarted, failed = context.isFailed)
        }

    override fun stored(): Set<String> = stores.stored()

    override fun complete(name: String): Boolean = stores.complete(name)

    override fun groups(): Set<UUID> = replication.localGroups()

    override fun catchingUp(id: NamedDatabaseId): Boolean = replication.catchingUp(id)

    override fun create(id: NamedDatabaseId) {
        manager.createDatabase(id)
    }

    override fun start(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = true
        manager.startDatabase(id)
    }

    override fun stop(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = false
        manager.stopDatabase(id)
    }

    override fun leave(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = true
        manager.stopDatabase(id)
    }

    override fun drop(id: NamedDatabaseId) {
        replication.replica(id)?.handOverOnStop = false
        manager.dropDatabase(id)
        replication.remove(id)
    }

    override fun deleteStore(name: String) = stores.delete(name)

    override fun removeGroup(id: UUID) = replication.removeGroup(id)

    override fun copy(source: HostPort, id: NamedDatabaseId) =
        stores.copyNew(source, DatabaseRef(id.name(), id.databaseId().uuid()))
}
