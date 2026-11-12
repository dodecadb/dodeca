package org.dozendb.cluster.internal

import org.dozendb.core.RemovedSettings
import org.neo4j.configuration.SettingMigrator
import org.neo4j.logging.InternalLog

/**
 * Refuses Neo4j Enterprise's removed `db.cluster.raft.leader_transfer.priority_group`, also of a
 * database, and `server.cluster.catchup.connect_randomly_to_server_group`, naming the settings that
 * replaced them ([RemovedSettings]). Registered in META-INF/services.
 */
class ClusterSettingsMigrator : SettingMigrator {
    override fun migrate(
        values: MutableMap<String, String>,
        defaultValues: MutableMap<String, String>,
        log: InternalLog,
    ) {
        RemovedSettings.refuse(
            values,
            log,
            "db.cluster.raft.leader_transfer.priority_group",
            DatabasePriorityTag.PREFIX,
            ofGroup = true,
        )
        RemovedSettings.refuse(
            values,
            log,
            "server.cluster.catchup.connect_randomly_to_server_group",
            ClusterSettings.connectRandomlyToServerTags.name(),
        )
    }
}
