package dev.dodeca.cluster.internal

import java.util.Locale
import org.neo4j.configuration.Config
import org.neo4j.configuration.Description
import org.neo4j.configuration.GroupSetting
import org.neo4j.configuration.GroupSettingHelper
import org.neo4j.configuration.SettingValueParsers.STRING
import org.neo4j.graphdb.config.Setting

/**
 * `db.cluster.raft.leader_transfer.priority_tag.<database>`: the server tag of the members that
 * should lead one database, in place of [ClusterSettings.leaderPriorityTag], as in Neo4j
 * Enterprise. A Neo4j group setting, registered in `META-INF/services`. Neo4j takes the database
 * name up to the next dot, so a database whose name has one gets no priority tag of its own.
 */
class DatabasePriorityTag private constructor(private val database: String?) : GroupSetting {
    /** The instance without a database, on which Neo4j finds the prefix. */
    constructor() : this(null)

    @JvmField
    @field:Description(
        "The server tag of the members that should lead this database, in place of " +
            "db.cluster.raft.leader_transfer.priority_tag."
    )
    val priorityTag: Setting<String>? = database?.let {
        GroupSettingHelper.getBuilder(PREFIX, it, STRING, null).build()
    }

    override fun name(): String? = database

    override fun getPrefix(): String = PREFIX

    companion object {
        const val PREFIX = "db.cluster.raft.leader_transfer.priority_tag"

        /** The setting of the priority tag of [database]. */
        fun of(database: String): Setting<String> =
            checkNotNull(DatabasePriorityTag(database).priorityTag)

        /**
         * The priority tag of each database in [config], by name: its own, or else
         * [ClusterSettings.leaderPriorityTag]. Names are matched as Neo4j matches database names,
         * ignoring case, so two settings whose database names differ only in case are refused.
         */
        fun lookup(config: Config): (String) -> String? {
            val own =
                config
                    .getGroups(DatabasePriorityTag::class.java)
                    .values
                    .mapNotNull { group ->
                        config.get(checkNotNull(group.priorityTag))?.let {
                            checkNotNull(group.name()).normalized() to it
                        }
                    }
                    .groupBy({ it.first }, { it.second })
                    .mapValues { (name, tags) ->
                        require(tags.size == 1) {
                            "$PREFIX is set more than once for the database $name"
                        }
                        tags.single()
                    }
            val default = config.get(ClusterSettings.leaderPriorityTag)
            return { name -> own[name.normalized()] ?: default }
        }

        private fun String.normalized(): String = lowercase(Locale.ROOT)
    }
}
