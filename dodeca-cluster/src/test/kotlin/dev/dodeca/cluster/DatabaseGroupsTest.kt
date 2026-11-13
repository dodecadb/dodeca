package dev.dodeca.cluster

import dev.dodeca.cluster.internal.of
import dev.dodeca.consensus.DatabaseRef
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.kernel.database.DatabaseId
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.kernel.database.NamedDatabaseId

class DatabaseGroupsTest {
    private val films = DatabaseIdFactory.from("films", UUID.randomUUID())
    private val system = NamedDatabaseId.NAMED_SYSTEM_DATABASE_ID

    @Test
    fun `a database's group has the database's id, system's too until a recovery re-formed it`() {
        val groups = DatabaseGroups()

        assertThat(groups.of(films)).isEqualTo(DatabaseRef("films", films.databaseId().uuid()))
        assertThat(groups.system).isEqualTo(DatabaseRef("system", DatabaseGroups.SYSTEM_DATABASE))
        assertThat(groups.of(system)).isEqualTo(groups.system)
        assertThat(DatabaseGroups.SYSTEM_DATABASE).isEqualTo(DatabaseId.SYSTEM_DATABASE_ID.uuid())
    }

    @Test
    fun `a re-formed system has a group of its own, which maps back to the database`() {
        val group = UUID.randomUUID()
        val groups = DatabaseGroups(group)

        assertThat(groups.of(system)).isEqualTo(DatabaseRef("system", group))
        assertThat(groups.of("system", DatabaseGroups.SYSTEM_DATABASE).id).isEqualTo(group)
        assertThat(groups.groupOf(DatabaseGroups.SYSTEM_DATABASE)).isEqualTo(group)
        assertThat(groups.databaseOf(group)).isEqualTo(DatabaseGroups.SYSTEM_DATABASE)
        val filmsId = films.databaseId().uuid()
        assertThat(groups.of(films).id).isEqualTo(filmsId)
        assertThat(groups.groupOf(filmsId)).isEqualTo(filmsId)
        assertThat(groups.databaseOf(filmsId)).isEqualTo(filmsId)
    }
}
