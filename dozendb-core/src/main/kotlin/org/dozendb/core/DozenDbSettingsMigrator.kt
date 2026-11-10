package org.dozendb.core

import org.neo4j.configuration.SettingMigrator
import org.neo4j.configuration.SettingMigrators
import org.neo4j.logging.InternalLog

/**
 * Takes Neo4j Enterprise's deprecated `server.groups` for `initial.server.tags`, which replaced it,
 * with Neo4j's warning. Registered in META-INF/services.
 */
class DozenDbSettingsMigrator : SettingMigrator {
    override fun migrate(
        values: MutableMap<String, String>,
        defaultValues: MutableMap<String, String>,
        log: InternalLog,
    ) {
        SettingMigrators.migrateSettingNameChange(
            values,
            log,
            "server.groups",
            DozenDbSettings.initialServerTags,
        )
    }
}
