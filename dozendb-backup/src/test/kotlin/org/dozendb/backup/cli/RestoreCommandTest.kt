package org.dozendb.backup.cli

import java.nio.file.Files
import java.nio.file.Path
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.backup.Admin
import org.dozendb.backup.Artifacts
import org.dozendb.backup.CommunityServer
import org.dozendb.backup.PhasedSource
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.dbms.identity.AbstractIdentityModule
import org.neo4j.dbms.identity.ServerId
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RestoreCommandTest {
    private lateinit var root: Path
    private lateinit var source: PhasedSource

    @BeforeAll
    fun writeSource(
        // Kept when a test fails, for the servers' logs.
        @TempDir(cleanup = CleanupMode.ON_SUCCESS) root: Path
    ) {
        this.root = root
        source = PhasedSource.write(root)
    }

    private fun restore(home: Path, vararg arguments: String) =
        Admin.run(home, ::RestoreCommand, *arguments)

    private fun restored(
        home: Path,
        database: String = CommunityServer.DATABASE,
    ): Map<String, Any?> =
        CommunityServer(home, database).start().use { server ->
            val state = PhasedSource.state(server)
            server.stop()
            assertThat(server.consistent()).describedAs("$home consistent").isTrue
            state
        }

    @Test
    fun `restores the latest chain of each database in a folder`() {
        val home = root.resolve("latest")

        val result = restore(home, "--from-path=${source.backups}")

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains(
                "Restored database 'neo4j' from ${source.full} and 2 differential backups, up to " +
                    "transaction ${source.lastTransactionId}, committed at ",
                "To use it, run CREATE DATABASE neo4j, or START DATABASE neo4j if it exists, on " +
                    "the system database.",
            )
        assertThat(restored(home)).isEqualTo(source.states[2])
    }

    @Test
    fun `restores the chain ending with a backup under another name, up to a transaction`() {
        val home = root.resolve("renamed")
        val until = source.phaseStarts[1].transactionId

        val result =
            restore(home, "--from-path=${source.second}", "--restore-until=$until", "films")

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(result.out)
            .contains("Restored database 'films'", "up to transaction ${until - 1},")
        assertThat(restored(home, "films")).isEqualTo(source.states[1])
    }

    @Test
    fun `restores only the database --source-database names, from folders and files`() {
        val mixed = Files.createDirectories(root.resolve("mixed"))
        for (file in listOf(source.full, source.first, source.second)) {
            Files.copy(file, mixed.resolve(file.fileName))
        }
        val films =
            CommunityServer(root.resolve("films-source"), "films").start().use { server ->
                CommunityServer.populate(server)
                server.backupFull(mixed.resolve("films-1-full${Artifacts.EXTENSION}"))
                CommunityServer.summary(server)
            }
        val home = root.resolve("source-database")
        val layout = Neo4jLayout.of(home)

        val folder = restore(home, "--from-path=$mixed", "--source-database=FILMS")

        assertThat(folder.exitCode).describedAs("$folder").isZero
        assertThat(folder.out).contains("Restored database 'films'").doesNotContain("'neo4j'")
        assertThat(layout.databaseLayout("neo4j").databaseDirectory()).doesNotExist()
        CommunityServer(home, "films").start().use {
            assertThat(CommunityServer.summary(it)).isEqualTo(films)
        }

        val file = restore(home, "--from-path=${source.second}", "--source-database=neo4j")
        assertThat(file.exitCode).describedAs("$file").isZero
        assertThat(restored(home)).isEqualTo(source.states[2])

        for ((arguments, message) in
            listOf(
                arrayOf("--from-path=${source.second}", "--source-database=films") to
                    "${source.second} is a backup of database 'neo4j', not of 'films' as " +
                        "--source-database names",
                arrayOf("--from-path=${source.backups}", "--source-database=films") to
                    "No backups of database 'films' found in ${source.backups}",
            )) {
            val result = restore(root.resolve("refused-source"), *arguments)
            assertThat(result.exitCode).describedAs("$result").isNotZero
            assertThat(result.err).contains(message)
        }
    }

    @Test
    fun `replaces a database only when told to and when it is stopped`() {
        val home = root.resolve("replaced")
        val until = "--restore-until=${source.phaseStarts[1].transactionId}"
        assertThat(restore(home, "--from-path=${source.backups}", until).exitCode).isZero

        val again = restore(home, "--from-path=${source.backups}")
        assertThat(again.exitCode).isNotZero
        assertThat(again.err)
            .contains(
                "Failed to restore database 'neo4j': database 'neo4j' exists at " +
                    "${Neo4jLayout.of(home).databaseLayout("neo4j").databaseDirectory()}: stop it " +
                    "and restore with --overwrite-destination=true to replace it"
            )
        CommunityServer(home).start().use {
            val running = restore(home, "--from-path=${source.backups}", "--overwrite-destination")
            assertThat(running.exitCode).isNotZero
            assertThat(running.err)
                .contains("the database is in use: stop database 'neo4j' and try again")
        }
        val replaced = restore(home, "--from-path=${source.backups}", "--overwrite-destination")

        assertThat(replaced.exitCode).describedAs("$replaced").isZero
        assertThat(restored(home)).isEqualTo(source.states[2])
    }

    @Test
    fun `restores to other directories`() {
        val home = root.resolve("directories")
        val databases = root.resolve("elsewhere/databases")
        val transactions = root.resolve("elsewhere/transactions")

        val result =
            restore(
                home,
                "--from-path=${source.backups}",
                "--to-path-data=$databases",
                "--to-path-txn=$transactions",
            )

        assertThat(result.exitCode).describedAs("$result").isZero
        assertThat(databases.resolve("neo4j/neostore")).exists()
        assertThat(Files.list(transactions.resolve("neo4j")).map { it.fileName.toString() })
            .anyMatch { it.startsWith("neostore.transaction.db.") }
        assertThat(Neo4jLayout.of(home).databaseLayout("neo4j").databaseDirectory()).doesNotExist()
    }

    @Test
    fun `with clustering enabled, prepares the database for CREATE DATABASE`() {
        val home = root.resolve("clustered")
        Files.createDirectories(home.resolve("conf"))
        Files.writeString(home.resolve("conf/neo4j.conf"), "dbms.dozendb.cluster.enabled=true\n")
        val layout = Neo4jLayout.of(home)

        val withoutId = restore(home, "--from-path=${source.backups}")

        assertThat(withoutId.exitCode).describedAs("$withoutId").isZero
        assertThat(withoutId.out)
            .contains("Prepared database 'neo4j' on this server from", "This server has no id yet.")
        assertThat(layout.databasesDirectory().resolve(".dozendb-seed-neo4j/neo4j/neostore"))
            .exists()
        assertThat(layout.databaseLayout("neo4j").databaseDirectory()).doesNotExist()

        val id = UUID.randomUUID()
        DefaultFileSystemAbstraction().use { fs ->
            AbstractIdentityModule.createServerIdStorage(fs, layout.serverIdFile())
                .writeState(ServerId(id))
        }
        val withId = restore(home, "--from-path=${source.backups}")
        assertThat(withId.out)
            .contains(
                "To create it, run on the system database:\n  CREATE DATABASE neo4j OPTIONS " +
                    "{existingData: 'use', existingDataSeedServer: '$id'}"
            )

        val overwrite = restore(home, "--from-path=${source.backups}", "--overwrite-destination")
        assertThat(overwrite.exitCode).isNotZero
        assertThat(overwrite.err)
            .contains("--overwrite-destination is not supported with clustering enabled")
    }

    @Test
    fun `refuses requests it cannot carry out`() {
        val home = root.resolve("refused")
        for ((arguments, message) in
            listOf(
                arrayOf("--from-path=${source.backups}", "--restore-until=yesterday") to
                    "Invalid --restore-until: 'yesterday' is neither a transaction id nor a time in UTC",
                arrayOf("--from-path=${source.backups}", "films") to
                    "A database name can be given only when --from-path names a single backup file",
                arrayOf("--from-path=${source.backups}", "--restore-until=2") to
                    "no full backup of neo4j holds only transactions before transaction 2",
                arrayOf("--from-path=${source.second}", "--restore-until=2") to
                    "the full backup neo4j-1-full.backup holds transactions up to",
            )) {
            val result = restore(home, *arguments)
            assertThat(result.exitCode).describedAs("$result").isNotZero
            assertThat(result.err).contains(message)
        }
    }
}
