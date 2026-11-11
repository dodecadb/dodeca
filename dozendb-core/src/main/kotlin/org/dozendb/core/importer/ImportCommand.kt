package org.dozendb.core.importer

import java.io.ByteArrayOutputStream
import java.io.OutputStream
import java.io.PrintStream
import org.neo4j.cli.CommandFailedException
import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext
import org.neo4j.cli.ExitCode
import org.neo4j.importer.ImportCommand
import org.neo4j.io.layout.Neo4jLayout
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.storageengine.api.StorageEngineFactory
import picocli.CommandLine.Command

/**
 * `neo4j-admin database import` with Neo4j's `incremental` subcommand next to `full`, which Neo4j
 * Community ships without registering it: DozenDB's [DozenDbIncrementalImporterFactory] does the
 * import. `full` is Neo4j Community's.
 */
@Command(
    name = "import",
    description = ["High-speed import of data from CSV files, optimized for fault-free data."],
    subcommands = [IncrementalImportCommand::class],
)
class DozenDbImportCommand : ImportCommand()

/**
 * Neo4j's `neo4j-admin database import incremental`, which fails with the reason DozenDB's importer
 * gives, rather than only "Error importing csv file.", and without Neo4j's warning that a failed
 * import leaves the store "likely in an unusable state": no stage leaves it so.
 */
@Command(name = "incremental", description = ["Incremental import into an existing database."])
class IncrementalImportCommand(context: ExecutionContext) :
    ImportCommand.Incremental(withoutUnusableStoreWarning(context)) {

    override fun execute() {
        val name = spec.positionalParameters().first().getValue<NormalizedDatabaseName>().name()
        val layout = Neo4jLayout.of(createPrefilledConfigBuilder().build()).databaseLayout(name)
        if (StorageEngineFactory.selectStorageEngine(ctx.fs(), layout).isEmpty) {
            throw CommandFailedException(
                "Database '$name' does not exist: an incremental import adds to an existing " +
                    "database. Create it with neo4j-admin database import full, or with CREATE " +
                    "DATABASE.",
                ExitCode.FAIL,
            )
        }
        try {
            super.execute()
        } catch (e: CommandFailedException) {
            val reason =
                generateSequence<Throwable>(e) { it.cause }
                    .filterIsInstance<IncrementalImportException>()
                    .firstOrNull() ?: throw e
            throw CommandFailedException(reason.message, e, reason.exitCode)
        }
    }

    private companion object {
        /** The line Neo4j's file importer prints when any import fails. */
        const val UNUSABLE_STORE_WARNING = "WARNING Import failed. The store files in "

        fun withoutUnusableStoreWarning(context: ExecutionContext): ExecutionContext =
            ExecutionContext(
                context.homeDir(),
                context.confDir(),
                context.out(),
                PrintStream(LineFilter(context.err()) { !it.startsWith(UNUSABLE_STORE_WARNING) }),
                context.`in`(),
                context.fs(),
            )
    }

    /** Passes on to [target] the lines that [keep] keeps. */
    private class LineFilter(
        private val target: PrintStream,
        private val keep: (String) -> Boolean,
    ) : OutputStream() {
        private val line = ByteArrayOutputStream()

        override fun write(b: Int) {
            line.write(b)
            if (b == '\n'.code) emit()
        }

        override fun flush() {
            target.flush()
        }

        override fun close() {
            if (line.size() > 0) emit()
            target.flush()
        }

        private fun emit() {
            val bytes = line.toByteArray()
            line.reset()
            if (keep(String(bytes))) target.write(bytes)
        }
    }
}

/** Provides [DozenDbImportCommand] in place of Neo4j Community's `import`. */
class DozenDbImportCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = DozenDbImportCommand()

    override fun commandType() = CommandType.IMPORT

    // Lower than Neo4j Community's, so that it takes precedence.
    override fun getPriority() = Int.MAX_VALUE - 1
}
