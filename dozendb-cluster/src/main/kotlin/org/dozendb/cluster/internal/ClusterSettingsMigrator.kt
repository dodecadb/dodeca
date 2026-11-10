package org.dozendb.cluster.internal

import org.neo4j.configuration.SettingMigrator
import org.neo4j.configuration.SettingMigrators
import org.neo4j.logging.InternalLog

/**
 * Takes Neo4j Enterprise's deprecated `db.cluster.raft.leader_transfer.priority_group`, also of a
 * database, for the priority tag, with Neo4j's warning. Registered in META-INF/services.
 */
class ClusterSettingsMigrator : SettingMigrator {
    override fun migrate(
        values: MutableMap<String, String>,
        defaultValues: MutableMap<String, String>,
        log: InternalLog,
    ) {
        SettingMigrators.migrateSettingNameChange(
            values,
            log,
            PRIORITY_GROUP,
            ClusterSettings.leaderPriorityTag,
        )
        SettingMigrators.migrateGroupSettingPrefixChange(
            values,
            log,
            PRIORITY_GROUP,
            DatabasePriorityTag.PREFIX,
        )
    }

    private companion object {
        const val PRIORITY_GROUP = "db.cluster.raft.leader_transfer.priority_group"
    }
}
