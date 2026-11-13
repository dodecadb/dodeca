package dev.dodeca.cluster.internal.routing

import java.util.Optional
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.DatabaseReferenceRepository
import org.neo4j.kernel.database.NormalizedCatalogEntry
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * The database references this member knows, which, before saying that it knows no database or
 * alias of a name, [confirm] that `system` here holds everything its leader had committed: a
 * database or alias that the leader created moments ago is then found. Neo4j's routing service
 * answers ROUTE requests with these, whose bookmarks it does not wait for. A name still unknown, or
 * one that the leader could not confirm in time, is not found.
 */
internal class ConfirmedReferences(
    private val delegate: DatabaseReferenceRepository,
    private val confirm: () -> Unit,
) : DatabaseReferenceRepository by delegate {
    override fun getByAlias(catalogEntry: NormalizedCatalogEntry): Optional<DatabaseReference> =
        confirmed {
            delegate.getByAlias(catalogEntry)
        }

    override fun getByAlias(databaseAlias: NormalizedDatabaseName): Optional<DatabaseReference> =
        confirmed {
            delegate.getByAlias(databaseAlias)
        }

    override fun getByAlias(databaseAlias: String): Optional<DatabaseReference> = confirmed {
        delegate.getByAlias(databaseAlias)
    }

    private fun <T : Any> confirmed(lookup: () -> Optional<T>): Optional<T> {
        val found = lookup()
        if (found.isPresent) return found
        try {
            confirm()
        } catch (_: RuntimeException) {
            return found
        }
        return lookup()
    }
}
