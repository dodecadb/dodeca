package org.dozendb.backup

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Path
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
        val context =
            ExecutionContext(
                home,
                home.resolve("conf"),
                PrintStream(out, true),
                PrintStream(err, true),
                input,
                DefaultFileSystemAbstraction(),
            )
        val exitCode = CommandLine(command(context)).execute(*arguments)
        return Result(exitCode, out.toString(), err.toString())
    }
}
