package org.dozendb.core.copy

import java.io.IOException
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration
import java.time.Instant
import org.dozendb.core.DozenDbSettings
import org.dozendb.core.importer.ScratchDbms
import org.dozendb.core.store.StorePlacement
import org.neo4j.batchimport.api.Configuration
import org.neo4j.batchimport.api.IndexConfig
import org.neo4j.batchimport.api.input.Collector
import org.neo4j.cli.ExitCode
import org.neo4j.commandline.dbms.LockChecker
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.dbms.identity.AbstractIdentityModule
import org.neo4j.importer.PrintingImportLogicMonitor
import org.neo4j.internal.batchimport.DefaultAdditionalIds
import org.neo4j.io.ByteUnit
import org.neo4j.io.fs.FileSystemAbstraction
import org.neo4j.io.layout.DatabaseLayout
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.io.locker.FileLockException
import org.neo4j.io.pagecache.context.CursorContextFactory
import org.neo4j.io.pagecache.context.FixedVersionContextSupplier
import org.neo4j.io.pagecache.tracing.PageCacheTracer
import org.neo4j.kernel.database.DatabaseTracers
import org.neo4j.kernel.impl.index.schema.IndexImporterFactoryImpl
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory
import org.neo4j.kernel.impl.store.format.RecordFormatSelector
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogInitializer
import org.neo4j.kernel.recovery.LogTailExtractor
import org.neo4j.kernel.recovery.Recovery
import org.neo4j.logging.NullLogProvider
import org.neo4j.logging.internal.SimpleLogService
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.storageengine.api.StorageEngineFactory
import org.neo4j.storageengine.api.TransactionIdStore

/**
 * The marker a copy leaves next to the database [layout] it made on a clustered server, which the
 * cluster reads before it starts the database: a copy is a store of its own, not one that the Raft
 * group of a database of that name wrote, and starts only as a seed.
 */
fun copiedMarker(layout: DatabaseLayout): Path =
    layout.neo4jLayout.databasesDirectory().resolve(".${layout.databaseName}.copied")

/** Fails a store copy with [message], which `neo4j-admin` prints as it is. */
internal class StoreCopyException(message: String, val exitCode: Int = ExitCode.FAIL) :
    RuntimeException(message)

/**
 * Copies the store of a stopped database to a new store, as Neo4j Enterprise's `neo4j-admin
 * database copy` does: its records are read and written anew by Neo4j's batch importer, which
 * compacts the stores and rebuilds the token lookup indexes; nodes keep their ids unless compacted,
 * relationships get new ones. Filters leave entities, labels and properties out; records that
 * cannot be read are left out and reported, which is how a copy leaves a store's inconsistencies
 * behind. The source is not written to.
 *
 * The other indexes and constraints come as Cypher statements, written to a file, or run on the
 * copy with [Request.copySchema]. A copy with the source's name goes to a staging place, next to
 * the database's own or under [Request.tempPath], and then replaces the source.
 *
 * On a clustered server, a copy with the source's name is marked ([copiedMarker]) before it takes
 * its place; one with another name is prepared where `CREATE DATABASE` with
 * `existingDataSeedServer` takes it from, as `neo4j-admin database restore` prepares a database
 * there.
 */
internal class StoreCopier(
    private val fs: FileSystemAbstraction,
    private val config: Config,
    private val out: PrintStream,
    private val err: PrintStream,
    private val verbose: Boolean,
) {
    class Request(
        val from: DatabaseLayout,
        val to: DatabaseLayout,
        val filters: CopyFilters,
        val compact: Boolean,
        val toFormat: String,
        val fromPageCache: Long,
        val copySchema: Boolean,
        val schemaDirectory: Path,
        /** Where a copy with the source's name is staged, if not next to the database. */
        val tempPath: Path?,
        val force: Boolean,
    )

    fun copy(request: Request) {
        val from = request.from
        val name = from.databaseName
        val toName = request.to.databaseName
        if (name == SYSTEM || toName == SYSTEM) {
            throw StoreCopyException(
                "neo4j-admin database copy copies user databases, not system.",
                ExitCode.USAGE,
            )
        }
        targetFormat(request.toFormat, "standard")
        val sourceEngine =
            StorageEngineFactory.selectStorageEngine(fs, from).orElseThrow {
                StoreCopyException(
                    "Database '$name' does not exist in ${from.databaseDirectory()}."
                )
            }
        val sameStore =
            from.databaseDirectory().toAbsolutePath().normalize() ==
                request.to.databaseDirectory().toAbsolutePath().normalize()
        if (!sameStore && StorageEngineFactory.selectStorageEngine(fs, request.to).isPresent) {
            throw StoreCopyException(
                "Database '$toName' already exists in ${request.to.databaseDirectory()}: copy to " +
                    "another name, or drop it first."
            )
        }
        lock(from).use { _ ->
            if (Recovery.isRecoveryRequired(fs, from, config, EmptyMemoryTracker.INSTANCE)) {
                if (!request.force) {
                    throw StoreCopyException(
                        "Database '$name' was not shut down cleanly and needs recovery: start " +
                            "and stop it to recover it, or copy its store as it is with --force, " +
                            "without the transactions that recovery would apply."
                    )
                }
                err.println(
                    "WARNING Database '$name' needs recovery: copying its store as it is, " +
                        "without the transactions that recovery would apply."
                )
            }
            val placement = StorePlacement(fs, request.to.neo4jLayout)
            val staging: Path? =
                request.tempPath?.takeIf { sameStore }?.resolve(".dozendb-copy-$name")
            val seed = !sameStore && clustered && servesFrom(request.to)
            val target =
                when {
                    sameStore && staging != null -> stagedLayout(staging, name)
                    sameStore -> placement.stage(name, StorePlacement.COPIED)
                    seed -> placement.stage(toName, StorePlacement.RESTORED)
                    else -> request.to
                }
            if (staging != null) fs.deleteRecursively(staging)
            try {
                fs.mkdirs(target.databaseDirectory())
                val statements = lock(target).use { _ -> copyStore(request, target, sourceEngine) }
                // The DBMS that creates the schema locks the copy itself.
                schema(request, target, statements)
            } catch (e: Throwable) {
                // Leave no half copy, which would be taken for a store.
                fs.deleteRecursively(target.databaseDirectory())
                fs.deleteRecursively(target.transactionLogsDirectory)
                staging?.let(fs::deleteRecursively)
                when {
                    sameStore && staging == null -> placement.unstage(name, StorePlacement.COPIED)
                    seed -> placement.unstage(toName, StorePlacement.RESTORED)
                }
                throw e
            }
            when {
                sameStore -> {
                    if (clustered) mark(from)
                    placement.moveIntoPlace(target, name)
                    staging?.let(fs::deleteRecursively)
                    out.println("Database '$name' is replaced by its copy.")
                }
                seed -> printCreate(toName)
            }
        }
    }

    private val clustered: Boolean
        get() = config.get(DozenDbSettings.clusterEnabled)

    /** Whether [layout] is where this server keeps its databases, not elsewhere. */
    private fun servesFrom(layout: DatabaseLayout): Boolean {
        val own = Neo4jLayout.of(config)
        return layout.neo4jLayout.databasesDirectory().toAbsolutePath().normalize() ==
            own.databasesDirectory().toAbsolutePath().normalize()
    }

    /**
     * Tells how to create database [name] from the copy prepared for it, as `neo4j-admin database
     * restore` does on a clustered server: the cluster takes a new database's store only from
     * there.
     */
    private fun printCreate(name: String) {
        out.println("Prepared database '$name' on this server.")
        val storage =
            AbstractIdentityModule.createServerIdStorage(fs, Neo4jLayout.of(config).serverIdFile())
        if (storage.exists()) {
            out.println(
                "To create it, run on the system database:\n  CREATE DATABASE $name OPTIONS " +
                    "{existingData: 'use', existingDataSeedServer: " +
                    "'${storage.readState().uuid()}'}"
            )
        } else {
            out.println(
                "This server has no id yet. Start it, then create the database with CREATE " +
                    "DATABASE $name OPTIONS {existingData: 'use', existingDataSeedServer: " +
                    "'<id>'}, where <id> is its memberId in CALL dozendb.cluster.members()."
            )
        }
    }

    /** Copies the store into [target]; returns the statements of its schema. */
    private fun copyStore(
        request: Request,
        target: DatabaseLayout,
        sourceEngine: StorageEngineFactory,
    ): SchemaStatements {
        val name = request.from.databaseName
        val toName = request.to.databaseName
        if (sourceEngine.name() != RECORD_ENGINE) {
            throw StoreCopyException(
                "Database '$name' is not in a record format, which neo4j-admin database copy reads."
            )
        }
        val report = CopyReport(request.schemaDirectory.resolve("$toName-copy.report"))
        val statements: SchemaStatements
        val input: StoreInput
        val started = System.nanoTime()
        // Closed once the importer read every entity, so that the report is whole.
        report.use { _ ->
            JobSchedulerFactory.createInitialisedScheduler().use { scheduler ->
                SourceStore(fs, request.from, config, scheduler, request.fromPageCache).use { source
                    ->
                    val latest = RecordFormatSelector.findLatestMinorVersion(source.formats, config)
                    if (latest != source.formats) {
                        throw StoreCopyException(
                            "Database '$name' is in format ${source.formats.name()}, of an earlier " +
                                "version: migrate it first, with neo4j-admin database migrate $name."
                        )
                    }
                    val format = targetFormat(request.toFormat, source.formats.formatFamily.name)
                    val dbConfig =
                        Config.newBuilder()
                            .fromConfig(config)
                            .set(GraphDatabaseSettings.db_format, format)
                            .build()
                    input = StoreInput(source, request.filters, request.compact, report)
                    StorageEngineFactory.selectStorageEngine(dbConfig)
                        .batchImporter(
                            target,
                            fs,
                            PageCacheTracer.NULL,
                            IMPORT_CONFIG,
                            SimpleLogService(NullLogProvider.getInstance()),
                            out,
                            verbose,
                            DefaultAdditionalIds.EMPTY,
                            dbConfig,
                            PrintingImportLogicMonitor(out, err),
                            scheduler,
                            Collector.STRICT,
                            TransactionLogInitializer.getLogFilesInitializer(),
                            IndexImporterFactoryImpl(),
                            EmptyMemoryTracker.INSTANCE,
                            CursorContextFactory(
                                PageCacheTracer.NULL,
                                FixedVersionContextSupplier(TransactionIdStore.BASE_TX_ID),
                            ),
                        )
                        .doImport(input)
                    statements =
                        SchemaStatements(
                            source.indexes,
                            source.constraints,
                            source.tokens,
                            request.filters,
                        )
                }
            }
        }
        out.println(
            "Copied ${count(input.nodesCopied.get(), "node")} and " +
                "${count(input.relationshipsCopied.get(), "relationship")} of database '$name' in " +
                "${Duration.ofNanos(System.nanoTime() - started).toSeconds()} s" +
                left(input, report)
        )
        if (report.unreadable > 0) {
            err.println(
                "WARNING ${count(report.unreadable, "entity", "entities")} of database '$name' " +
                    "could not be read and were left out of the copy: they are listed in " +
                    "${report.file.toAbsolutePath()}."
            )
        }
        for (description in statements.leftOut) {
            out.println("Left out the $description, on what the filters left out.")
        }
        return statements
    }

    /** Marks the copy that takes the place of database [layout] on a clustered server. */
    private fun mark(layout: DatabaseLayout) {
        val marker = copiedMarker(layout)
        Files.createDirectories(marker.parent)
        Files.writeString(marker, "Copied by neo4j-admin database copy at ${Instant.now()}\n")
    }

    /** Creates the schema of the copy in [target], or writes its statements to a file. */
    private fun schema(request: Request, target: DatabaseLayout, statements: SchemaStatements) {
        val toName = request.to.databaseName
        if (request.copySchema) {
            createSchema(target, statements.statements)
        } else {
            val file = request.schemaDirectory.resolve("$toName-schema.cypher")
            Files.createDirectories(file.toAbsolutePath().parent)
            Files.write(file, statements.statements.map { "$it;" })
            out.println(
                "The statements that create the indexes and constraints of the copy are in " +
                    "${file.toAbsolutePath()}."
            )
        }
    }

    /**
     * What the copy left out: the entities the filters leave out, with the relationships of the
     * nodes left out, and the entities that could not be read.
     */
    private fun left(input: StoreInput, report: CopyReport): String {
        val parts =
            listOfNotNull(
                input.nodesLeftOut.get().takeIf { it > 0 }?.let { count(it, "node") },
                input.relationshipsLeftOut
                    .get()
                    .takeIf { it > 0 }
                    ?.let {
                        count(it, "relationship")
                    },
            )
        val leftOut = if (parts.isEmpty()) "" else ", leaving out ${parts.joinToString(" and ")}"
        val unreadable =
            if (report.unreadable > 0) {
                "; ${count(report.unreadable, "entity", "entities")} could not be read"
            } else {
                ""
            }
        return "$leftOut$unreadable."
    }

    private fun count(n: Long, one: String, many: String = "${one}s") =
        "$n ${if (n == 1L) one else many}"

    /** Runs [statements] on the copy in [target], and waits for its indexes to be built. */
    private fun createSchema(target: DatabaseLayout, statements: List<String>) {
        if (statements.isEmpty()) return
        val engine = StorageEngineFactory.selectStorageEngine(fs, target).orElseThrow()
        val tail =
            LogTailExtractor(fs, config, engine, DatabaseTracers.EMPTY)
                .getTailMetadata(target, EmptyMemoryTracker.INSTANCE)
        val scratch =
            target.neo4jLayout.databasesDirectory().resolve(".${target.databaseName}.copy")
        ScratchDbms.open(
                target,
                config,
                scratch.resolve("home"),
                scratch.resolve("logs"),
                writable = true,
                TransactionIdStore.UNKNOWN_CONSENSUS_INDEX,
                tail.kernelVersion(),
                ByteUnit.mebiBytes(512),
            )
            .use { dbms ->
                for (statement in statements) dbms.database.executeTransactionally(statement)
                dbms.database.executeTransactionally(
                    "CALL db.awaitIndexes(\$seconds)",
                    mapOf("seconds" to INDEX_WAIT_SECONDS),
                )
            }
        ScratchDbms.deleteHome(scratch)
        out.println("Created ${statements.size} indexes and constraints on the copy.")
    }

    private fun targetFormat(toFormat: String, sourceFamily: String): String {
        val format = if (toFormat == "same") sourceFamily else toFormat
        return when (format) {
            "standard",
            "aligned" -> format
            "high_limit",
            "block" ->
                throw StoreCopyException(
                    "DozenDB stores databases in the standard and aligned formats, not $format.",
                    ExitCode.USAGE,
                )
            else ->
                throw StoreCopyException(
                    "Unknown format $format: use same, standard or aligned.",
                    ExitCode.USAGE,
                )
        }
    }

    /** Locks [layout], which must not be in use. */
    private fun lock(layout: DatabaseLayout): AutoCloseable =
        try {
            LockChecker.checkDatabaseLock(layout)
        } catch (e: FileLockException) {
            throw StoreCopyException(
                "Database '${layout.databaseName}' is in use. Stop it first, with STOP DATABASE " +
                    "${layout.databaseName}."
            )
        } catch (e: IOException) {
            throw StoreCopyException("Cannot lock database '${layout.databaseName}': ${e.message}")
        }

    /** A layout for database [name] under [staging]. */
    private fun stagedLayout(staging: Path, name: String): DatabaseLayout =
        Neo4jLayout.of(
                Config.newBuilder()
                    .set(
                        GraphDatabaseSettings.neo4j_home,
                        config.get(GraphDatabaseSettings.neo4j_home),
                    )
                    .set(
                        GraphDatabaseInternalSettings.databases_root_path,
                        staging.resolve("databases"),
                    )
                    .set(
                        GraphDatabaseSettings.transaction_logs_root_path,
                        staging.resolve("transactions"),
                    )
                    .build()
            )
            .databaseLayout(name)

    private companion object {
        const val SYSTEM = "system"
        const val RECORD_ENGINE = "record"
        const val INDEX_WAIT_SECONDS = 86_400L

        /** Neo4j's import defaults, with the token lookup indexes built. */
        val IMPORT_CONFIG: Configuration =
            object : Configuration.Overridden(Configuration.defaultConfiguration()) {
                override fun indexConfig(): IndexConfig =
                    IndexConfig.create().withLabelIndex().withRelationshipTypeIndex()
            }
    }
}
