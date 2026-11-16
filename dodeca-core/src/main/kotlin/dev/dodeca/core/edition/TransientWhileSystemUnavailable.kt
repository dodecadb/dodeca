package dev.dodeca.core.edition

import java.util.Optional
import java.util.UUID
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider.SystemDatabaseUnavailableException
import org.neo4j.graphdb.DatabaseShutdownException
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceImpl
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * The database references of [delegate], whose lookups fail with the transient error of an
 * unavailable database while `system`, which holds the databases and aliases, is unavailable, as
 * while the server stops. Neo4j Community's query router lets the error of the system database's
 * provider, which has no status, reach Bolt, which logs it as an unexpected error and cannot send
 * it back: the client waits until its connection times out. Drivers retry the transient error, on
 * another server where there is one.
 */
internal class TransientWhileSystemUnavailable(private val delegate: DatabaseReferenceRepository) :
    DatabaseReferenceRepository {
    override fun getByAlias(catalogEntry: NormalizedCatalogEntry): Optional<DatabaseReference> =
        transient {
            delegate.getByAlias(catalogEntry)
        }

    override fun getByAlias(databaseAlias: NormalizedDatabaseName): Optional<DatabaseReference> =
        transient {
            delegate.getByAlias(databaseAlias)
        }

    override fun getByAlias(databaseAlias: String): Optional<DatabaseReference> = transient {
        delegate.getByAlias(databaseAlias)
    }

    override fun getByDisplayName(
        displayName: NormalizedDatabaseName
    ): Optional<DatabaseReference> = transient { delegate.getByDisplayName(displayName) }

    override fun getByDisplayName(displayName: String): Optional<DatabaseReference> = transient {
        delegate.getByDisplayName(displayName)
    }

    override fun getByUuid(uuid: UUID): Optional<DatabaseReference> = transient {
        delegate.getByUuid(uuid)
    }

    override fun getAllDatabaseReferences(): Set<DatabaseReference> = transient {
        delegate.allDatabaseReferences
    }

    override fun getCompositeDatabaseReferences(): Set<DatabaseReferenceImpl.Composite> =
        transient {
            delegate.compositeDatabaseReferences
        }

    private inline fun <T> transient(lookup: () -> T): T =
        try {
            lookup()
        } catch (e: SystemDatabaseUnavailableException) {
            throw DatabaseShutdownException.databaseUnavailable(
                GraphDatabaseSettings.SYSTEM_DATABASE_NAME,
                e,
            )
        }
}
