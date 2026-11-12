package org.dozendb.core.copy

import org.neo4j.configuration.Config
import org.neo4j.internal.id.ScanOnOpenReadOnlyIdGeneratorFactory
import org.neo4j.internal.recordstorage.RecordStorageReader
import org.neo4j.internal.recordstorage.SchemaRuleAccess
import org.neo4j.internal.recordstorage.StoreTokens
import org.neo4j.internal.schema.ConstraintDescriptor
import org.neo4j.internal.schema.IndexDescriptor
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout
import org.neo4j.io.pagecache.PageCache
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache
import org.neo4j.io.pagecache.impl.muninn.StandalonePageCacheFactory
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.DatabaseCreationOptions
import org.neo4j.kernel.impl.store.NeoStores
import org.neo4j.kernel.impl.store.StoreFactory
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors
import org.neo4j.kernel.impl.store.format.RecordFormatSelector
import org.neo4j.kernel.impl.store.format.RecordFormats
import org.neo4j.logging.NullLogProvider
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.scheduler.JobScheduler
import org.neo4j.token.TokenHolders

/**
 * The record store of [layout], a stopped database, opened read-only through a page cache of
 * [pageCacheSize] bytes of its own: its records, tokens and schema, as `neo4j-admin database copy`
 * reads them. Nothing is written to it.
 */
internal class SourceStore(
    fs: FileSystemAbstraction,
    layout: DatabaseLayout,
    config: Config,
    scheduler: JobScheduler,
    pageCacheSize: Long,
) : AutoCloseable {
    val contextFactory: CursorContextFactory = CursorContextFactory.NULL_CONTEXT_FACTORY
    private val recordLayout = RecordDatabaseLayout.convert(layout)
    private val pageCache: PageCache =
        StandalonePageCacheFactory.createPageCache(
            fs,
            scheduler,
            PageCacheTracer.NULL,
            MuninnPageCache.forMemory(pageCacheSize),
        )

    /** The store's record format. */
    val formats: RecordFormats

    val neoStores: NeoStores

    /** The store's tokens. */
    val tokens: TokenHolders

    /** The store's indexes and constraints. */
    val indexes: List<IndexDescriptor>
    val constraints: List<ConstraintDescriptor>

    init {
        try {
            formats =
                RecordFormatSelector.selectForStore(
                    recordLayout,
                    fs,
                    pageCache,
                    NullLogProvider.getInstance(),
                    contextFactory,
                )
            neoStores =
                StoreFactory(
                        recordLayout,
                        config,
                        ScanOnOpenReadOnlyIdGeneratorFactory(),
                        pageCache,
                        PageCacheTracer.NULL,
                        fs,
                        formats,
                        NullLogProvider.getInstance(),
                        contextFactory,
                        true,
                        DatabaseCreationOptions.EMPTY_CREATION_OPTIONS,
                    )
                    .openAllNeoStores()
            try {
                cursors().use { cursors ->
                    tokens =
                        StoreTokens.readOnlyTokenHolders(
                            neoStores,
                            cursors,
                            EmptyMemoryTracker.INSTANCE,
                        )
                    val schema = SchemaRuleAccess.getSchemaRuleAccess(neoStores.schemaStore, tokens)
                    indexes =
                        schema
                            .indexesGetAllIgnoreMalformed(cursors, EmptyMemoryTracker.INSTANCE)
                            .asSequence()
                            .toList()
                    constraints =
                        schema
                            .constraintsGetAllIgnoreMalformed(cursors, EmptyMemoryTracker.INSTANCE)
                            .asSequence()
                            .toList()
                }
            } catch (e: Exception) {
                neoStores.close()
                throw e
            }
        } catch (e: Exception) {
            pageCache.close()
            throw e
        }
    }

    /** Store cursors for one thread, with a cursor context of their own. */
    fun cursors(): CachedStoreCursors =
        CachedStoreCursors(neoStores, contextFactory.create("dozendb-copy"))

    /** A reader of the store's entities for one thread. */
    fun reader(): RecordStorageReader = RecordStorageReader(neoStores)

    /** The highest node id in use, plus one. */
    val nodeHighId: Long
        get() = neoStores.nodeStore.idGenerator.highId

    /** The highest relationship id in use, plus one. */
    val relationshipHighId: Long
        get() = neoStores.relationshipStore.idGenerator.highId

    override fun close() {
        try {
            neoStores.close()
        } finally {
            pageCache.close()
        }
    }
}
