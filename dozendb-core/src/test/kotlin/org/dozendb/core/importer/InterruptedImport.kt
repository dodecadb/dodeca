package org.dozendb.core.importer

import java.io.InputStream
import java.nio.file.Path
import kotlin.system.exitProcess
import org.dozendb.core.importer.ResumableBatchImporter.Point
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/**
 * The `import` command as `neo4j-admin` builds it, with DozenDB's `full` in place of Neo4j
 * Community's, whose import calls [interruption] at the points it may stop at.
 */
internal fun importCommand(context: ExecutionContext, interruption: (Point) -> Unit): CommandLine {
    val factory =
        object : CommandLine.IFactory {
            override fun <K> create(type: Class<K>): K =
                when {
                    type == FullImportCommand::class.java ->
                        type.cast(
                            FullImportCommand(context).also { it.interruption = interruption }
                        )
                    type.constructors.any {
                        it.parameterTypes.contentEquals(arrayOf(ExecutionContext::class.java))
                    } -> type.getConstructor(ExecutionContext::class.java).newInstance(context)
                    else -> CommandLine.defaultFactory().create(type)
                }
        }
    return CommandLine(DozenDbImportCommand::class.java, factory)
}

/**
 * Runs `neo4j-admin database import full` with the arguments after the first two, for the server
 * whose home is the second, until the point the first names, where it halts the JVM as a crash
 * would: nothing is closed or flushed.
 */
internal object CrashingImport {
    const val CRASHED = 137

    @JvmStatic
    fun main(arguments: Array<String>) {
        val point = Point.valueOf(arguments[0])
        val home = Path.of(arguments[1])
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                System.out,
                System.err,
                InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val command =
            importCommand(context) {
                if (it == point) {
                    System.out.flush()
                    Runtime.getRuntime().halt(CRASHED)
                }
            }
        exitProcess(command.execute("full", *arguments.drop(2).toTypedArray()))
    }
}
