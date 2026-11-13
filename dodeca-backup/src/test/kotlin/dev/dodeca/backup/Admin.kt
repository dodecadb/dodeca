package dev.dodeca.backup

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.io.PrintWriter
import java.nio.file.Path
import org.neo4j.cli.AdminTool
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/** Runs `neo4j-admin` commands in this JVM, for the server in [home]. */
internal object Admin {
    class Result(val exitCode: Int, val out: String, val err: String) {
        override fun toString() = "exit code $exitCode\n$out\n$err"
    }

    fun run(
        home: Path,
        command: (ExecutionContext) -> Any,
        vararg arguments: String,
        input: InputStream = InputStream.nullInputStream(),
    ): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val errors = PrintStream(err, true)
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                PrintStream(out, true),
                errors,
                input,
                DefaultFileSystemAbstraction(),
            )
        // Usage errors too, which picocli prints itself.
        val exitCode =
            CommandLine(command(context)).setErr(PrintWriter(errors, true)).execute(*arguments)
        return Result(exitCode, out.toString(), err.toString())
    }

    /**
     * Runs `neo4j-admin` [arguments] as Neo4j's tool dispatches them, to the commands of every
     * provider on the classpath, by group and name: `backup aggregate`, `database backup` and so
     * on.
     */
    fun tool(home: Path, vararg arguments: String): Result {
        val out = ByteArrayOutputStream()
        val err = ByteArrayOutputStream()
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                PrintStream(out, true),
                PrintStream(err, true),
                InputStream.nullInputStream(),
                DefaultFileSystemAbstraction(),
            )
        val exitCode = AdminTool.execute(context, *arguments)
        return Result(exitCode, out.toString(), err.toString())
    }
}
