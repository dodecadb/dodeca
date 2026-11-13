package dev.dodeca.core.edition

import java.nio.file.Files
import java.nio.file.Path
import java.time.Instant
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.database.DatabaseId

class QuarantinesTest {
    @TempDir lateinit var root: Path

    private val file: Path
        get() = root.resolve("data").resolve(Quarantines.FILE_NAME)

    private val films = UUID.fromString("00000000-0000-0000-0000-00000000f11e")
    private val books = UUID.fromString("00000000-0000-0000-0000-0000000b0015")
    private val time = Instant.parse("2026-11-10T09:08:07.006Z")

    @Test
    fun `the message is Neo4j Enterprise's, the time in UTC with milliseconds`() {
        assertThat(Quarantine("neo4j", time, "Disk failed").message)
            .isEqualTo("By neo4j at 2026-11-10T09:08:07.006Z: Disk failed")
        assertThat(Quarantine("neo4j", Instant.parse("2026-11-10T09:08:07Z"), "").message)
            .isEqualTo("By neo4j at 2026-11-10T09:08:07.000Z: No reason given")
    }

    @Test
    fun `quarantines of user databases are kept in the file, line breaks and tabs as spaces`() {
        Quarantines(file).apply {
            put(films, Quarantine("ops\tteam", time, "first line\nsecond\tline"))
            put(books, Quarantine("neo4j", time, ""))
        }

        val read = Quarantines(file)

        assertThat(read.all())
            .containsOnly(
                java.util.Map.entry(films, Quarantine("ops team", time, "first line second line")),
                java.util.Map.entry(books, Quarantine("neo4j", time, "")),
            )
        assertThat(read.remove(films)).isNotNull
        assertThat(read.remove(films)).isNull()
        assertThat(Quarantines(file).all().keys).containsExactly(books)
    }

    @Test
    fun `system's is kept in memory only`() {
        val system = DatabaseId.SYSTEM_DATABASE_ID.uuid()
        val quarantines = Quarantines(file)

        quarantines.put(system, Quarantine("neo4j", time, "Inspecting it"))

        assertThat(quarantines.of(system)).isNotNull
        assertThat(file).doesNotExist()
        assertThat(Quarantines(file).of(system)).isNull()
        assertThat(quarantines.remove(system)).isNotNull
        assertThat(quarantines.of(system)).isNull()
    }

    @Test
    fun `forgets the quarantines of databases that are gone`() {
        val quarantines = Quarantines(file)
        quarantines.put(films, Quarantine("neo4j", time, ""))
        quarantines.put(books, Quarantine("neo4j", time, ""))

        quarantines.retainOnly(setOf(books))

        assertThat(Quarantines(file).all().keys).containsExactly(books)
    }

    @Test
    fun `a file that cannot be read fails with its name and how to repair it`() {
        Files.createDirectories(file.parent)
        Files.writeString(file, "$films\tneo4j\tyesterday\treason\n")

        assertThatThrownBy { Quarantines(file).of(films) }
            .hasMessageContaining("$file holds a line that is not a quarantine")
            .hasMessageContaining("delete the line to lift that quarantine")
    }
}
