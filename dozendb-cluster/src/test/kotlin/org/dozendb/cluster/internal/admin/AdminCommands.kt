package org.dozendb.cluster.internal.admin

import java.io.ByteArrayOutputStream
import java.io.InputStream
import java.io.PrintStream
import java.nio.file.Path
import org.neo4j.cli.ExecutionContext
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import picocli.CommandLine

/** What a `neo4j-admin` command printed, and its exit code. */
internal class CommandResult(val exitCode: Int, val out: String, val err: String) {
    override fun toString() = "exit code $exitCode\n$out\n$err"
}

/** Runs `neo4j-admin dbms unbind-system-db` with [arguments] for the server in [home]. */
internal fun unbindSystemDb(home: Path, vararg arguments: String): CommandResult {
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
    val exitCode = CommandLine(UnbindSystemCommand(context)).execute(*arguments)
    return CommandResult(exitCode, out.toString(), err.toString())
}
