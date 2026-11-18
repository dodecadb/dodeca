package dev.dodeca.backup

import dev.dodeca.core.store.CommittedTransaction
import java.nio.file.Files
import java.nio.file.Path
import org.neo4j.dbms.archive.DumpFormatSelector
import org.neo4j.dbms.archive.Dumper
import org.neo4j.io.fs.DefaultFileSystemAbstraction

/**
 * Backups of a database written in three phases, in [backups]: a dump and a full backup after the
 * first phase, a differential backup after each of the others, and an empty one after that.
 */
internal class PhasedSource private constructor(val backups: Path) {
    val dump: Path = backups.resolve("neo4j.dump")
    val full: Path = backups.resolve("neo4j-1-full${Artifacts.EXTENSION}")
    val first: Path = backups.resolve("neo4j-2-diff${Artifacts.EXTENSION}")
    val second: Path = backups.resolve("neo4j-3-diff${Artifacts.EXTENSION}")
    val empty: Path = backups.resolve("neo4j-4-empty${Artifacts.EXTENSION}")

    /** What the database holds after each phase. */
    lateinit var states: List<Map<String, Any?>>
        private set

    /** The first transaction of the second and of the third phase. */
    lateinit var phaseStarts: List<CommittedTransaction>
        private set

    var lastTransactionId = 0L
        private set

    companion object {
        /** Writes the database in [root]`/source` and its backups in [root]`/backups`. */
        fun write(root: Path): PhasedSource {
            val source = PhasedSource(Files.createDirectories(root.resolve("backups")))
            CommunityServer(root.resolve("source")).start().use { server ->
                CommunityServer.populate(server)
                val states = mutableListOf(state(server))
                val fullHeader = server.backupFull(source.full)
                server.stop()
                DefaultFileSystemAbstraction().use { fs ->
                    Dumper(fs)
                        .dump(
                            Dumper.FileOutput.of(fs, source.dump),
                            DumpFormatSelector.selectWriteFormat(),
                            Dumper.collectManifest(
                                server.layout.databaseDirectory(),
                                server.layout.transactionLogsDirectory,
                            ) {
                                it.fileName == server.layout.databaseLockFile().fileName
                            },
                        )
                }
                server.start()
                val starts = mutableListOf<CommittedTransaction>()
                var after = fullHeader.highestAppendIndex
                for ((phase, file) in listOf("B" to source.first, "C" to source.second)) {
                    // Every transaction of a phase commits after every one of the phase before.
                    Thread.sleep(10)
                    write(server, phase)
                    starts += server.transactionsAfter(after).first()
                    states += state(server)
                    after = server.backupDifferential(after, file).highestAppendIndex
                }
                check(server.backupDifferential(after, source.empty).empty)
                source.states = states
                source.phaseStarts = starts
                source.lastTransactionId = server.lastCommitted().id()
            }
            return source
        }

        private fun write(server: CommunityServer, phase: String) {
            val database = server.database()
            database.executeTransactionally(
                if (phase == "B") "CREATE INDEX person_phase_B FOR (p:Person) ON (p.phase)"
                else "CREATE TEXT INDEX person_phase_C FOR (n:Later) ON (n.phase)"
            )
            for (from in 0 until 300 step 50) {
                database.executeTransactionally(
                    "MATCH (p:Person) WHERE p.id >= ${'$'}from AND p.id < ${'$'}from + 50 " +
                        "SET p.phase = ${'$'}phase",
                    mapOf("from" to from, "phase" to phase),
                )
            }
            database.executeTransactionally(
                "UNWIND range(1, 20) AS n CREATE (:Later {phase: ${'$'}phase, n: n})",
                mapOf("phase" to phase),
            )
            database.executeTransactionally("MATCH (p:Person) WHERE p.id % 11 = 0 DETACH DELETE p")
        }

        /**
         * What [server] holds, as queries that use every index find it, once its indexes are
         * online: an index created just before, as a phase's, populates in the background, here and
         * in a database restored from a backup that has it.
         */
        fun state(server: CommunityServer): Map<String, Any?> {
            server.database().executeTransactionally("CALL db.awaitIndexes(60)")
            return CommunityServer.summary(server) +
                server
                    .rows(
                        """
                        CALL { MATCH (p:Person) WHERE p.phase = 'B' RETURN count(p) AS phaseB }
                        CALL { MATCH (p:Person) WHERE p.phase = 'C' RETURN count(p) AS phaseC }
                        CALL { MATCH (n:Later) RETURN count(n) AS later }
                        RETURN phaseB, phaseC, later
                        """
                            .trimIndent()
                    )
                    .single() +
                server
                    .rows(
                        "SHOW INDEXES YIELD name, state WHERE name STARTS WITH 'person_phase' " +
                            "RETURN collect(name + ' ' + state) AS phaseIndexes"
                    )
                    .single()
        }
    }
}
