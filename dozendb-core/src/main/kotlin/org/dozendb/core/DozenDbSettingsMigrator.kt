package org.dozendb.core

import org.neo4j.configuration.SettingMigrator
import org.neo4j.logging.InternalLog

/**
 * Refuses Neo4j Enterprise's removed `server.groups`, naming `initial.server.tags`, which replaced
 * it ([RemovedSettings]). Registered in META-INF/services.
 */
class DozenDbSettingsMigrator : SettingMigrator {
    override fun migrate(
        values: MutableMap<String, String>,
        defaultValues: MutableMap<String, String>,
        log: InternalLog,
    ) {
        RemovedSettings.refuse(
            values,
            log,
            "server.groups",
            DozenDbSettings.initialServerTags.name(),
        )
    }
}
