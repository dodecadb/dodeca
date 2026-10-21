package org.dozendb.core.store

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.io.fs.DefaultFileSystemAbstraction
import org.neo4j.io.layout.Neo4jLayout

class StorePlacementTest {
    @TempDir lateinit var home: Path
    private val fs = DefaultFileSystemAbstraction()
    private val layout by lazy { Neo4jLayout.of(home) }
    private val placement by lazy { StorePlacement(fs, layout) }

    @Test
    fun `stages next to the database's directories and moves the files into place`() {
        val target = layout.databaseLayout("films")
        Files.createDirectories(target.databaseDirectory()).resolve("old").also(Files::createFile)
        Files.createDirectories(target.transactionLogsDirectory)
            .resolve("old")
            .also(Files::createFile)

        val staged = placement.stage("films", "restore")
        assertThat(staged.databaseDirectory().parent.parent).isEqualTo(layout.databasesDirectory())
        assertThat(staged.transactionLogsDirectory.parent.parent)
            .isEqualTo(layout.transactionLogsRootDirectory())
        assertThat(staged.databaseDirectory()).isEmptyDirectory()
        Files.createFile(staged.databaseDirectory().resolve("neostore"))
        Files.createFile(staged.transactionLogsDirectory.resolve("neostore.transaction.db.0"))
        placement.moveIntoPlace(staged, "films")

        assertThat(Files.list(target.databaseDirectory()).map { it.fileName.toString() })
            .containsExactly("neostore")
        assertThat(Files.list(target.transactionLogsDirectory).map { it.fileName.toString() })
            .containsExactly("neostore.transaction.db.0")
        assertThat(staged.databaseDirectory().parent).doesNotExist()
        assertThat(staged.transactionLogsDirectory.parent).doesNotExist()
        assertThat(placement.incomplete("films")).isFalse
    }

    @Test
    fun `a new staging place drops what an earlier one held`() {
        val earlier = placement.stage("films", "seed")
        Files.createFile(earlier.databaseDirectory().resolve("neostore"))

        val staged = placement.stage("films", "seed")

        assertThat(staged.databaseDirectory()).isEmptyDirectory()
    }

    @Test
    fun `a database left half-moved is incomplete until deleted`() {
        // As a move interrupted after the marker was written.
        Files.createDirectories(layout.databasesDirectory())
        Files.createFile(layout.databasesDirectory().resolve(".dozendb-incomplete-films"))

        assertThat(placement.incomplete("films")).isTrue
        assertThat(placement.markedDatabase(".dozendb-incomplete-films")).isEqualTo("films")
        assertThat(placement.markedDatabase("films")).isNull()
        placement.delete("films")
        assertThat(placement.incomplete("films")).isFalse
    }
}
