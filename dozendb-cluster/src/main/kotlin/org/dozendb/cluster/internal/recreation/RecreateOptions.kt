package org.dozendb.cluster.internal.recreation

import org.dozendb.core.Gql

/**
 * The options of `dbms.cluster.recreateDatabase`, as Neo4j Enterprise documents them.
 *
 * @property seedUri the backup or dump every copy's store is replaced by; with [seedingServers],
 *   the fallback when no seeder holds a store.
 * @property seedingServers the servers whose stores are synchronised to the most up-to-date of
 *   them, by id; empty for every server hosting a copy that is enabled; null if not given.
 * @property primaries the new number of primaries; null to keep the topology.
 * @property secondaries the new number of secondaries; null to keep the topology, 0 when only
 *   [primaries] is given.
 */
internal data class RecreateOptions(
    val seedUri: String?,
    val seedingServers: List<String>?,
    val primaries: Int?,
    val secondaries: Int?,
) {
    companion object {
        const val SEED_URI = "seedURI"
        const val SEEDING_SERVERS = "seedingServers"
        const val PRIMARIES = "primaries"
        const val SECONDARIES = "secondaries"
        private val KEYS = listOf(SEED_URI, SEEDING_SERVERS, PRIMARIES, SECONDARIES)

        /** The most primaries and secondaries a database can have, as in Neo4j Enterprise. */
        const val MAXIMUM_PRIMARIES = 11
        const val MAXIMUM_SECONDARIES = 20

        /**
         * Reads [options] for recreating [database], refusing, as [failed] says, unknown keys,
         * values of the wrong type, neither seed nor both with servers named, and secondaries
         * without primaries.
         */
        fun parse(options: Map<String, Any?>, failed: String): RecreateOptions {
            val unknown = options.keys.filter { it !in KEYS }
            if (unknown.isNotEmpty()) {
                throw Gql.refused(
                    "$failed: unknown option${if (unknown.size == 1) "" else "s"} " +
                        "${unknown.joinToString { "'$it'" }}. The options are " +
                        "${KEYS.joinToString { "'$it'" }}."
                )
            }
            val seedUri =
                options[SEED_URI]?.let {
                    it as? String ?: throw Gql.refused("$failed: $SEED_URI must be a string.")
                }
            val seedingServers =
                options[SEEDING_SERVERS]?.let { value ->
                    (value as? List<*>)?.map {
                        it as? String
                            ?: throw Gql.refused(
                                "$failed: $SEEDING_SERVERS must be a list of server ids."
                            )
                    }
                        ?: throw Gql.refused(
                            "$failed: $SEEDING_SERVERS must be a list of server ids."
                        )
                }
            if (seedUri == null && seedingServers == null) {
                throw Gql.refused(
                    "$failed: give $SEEDING_SERVERS, the servers whose stores to recreate it " +
                        "from, or $SEED_URI, a backup or dump to recreate it from."
                )
            }
            if (seedUri != null && !seedingServers.isNullOrEmpty()) {
                throw Gql.refused(
                    "$failed: with $SEED_URI, $SEEDING_SERVERS must be empty: the seed is then " +
                        "the fallback when none of the servers hosting a copy holds a store."
                )
            }
            val primaries = count(options, PRIMARIES, 1..MAXIMUM_PRIMARIES, failed)
            val secondaries = count(options, SECONDARIES, 0..MAXIMUM_SECONDARIES, failed)
            if (secondaries != null && primaries == null) {
                throw Gql.refused(
                    "$failed: $SECONDARIES needs $PRIMARIES, which give the new topology."
                )
            }
            return RecreateOptions(
                seedUri,
                seedingServers,
                primaries,
                secondaries ?: primaries?.let { 0 },
            )
        }

        @Suppress("DEPRECATION")
        private fun count(
            options: Map<String, Any?>,
            key: String,
            range: IntRange,
            failed: String,
        ): Int? {
            val value = options[key] ?: return null
            val number =
                (value as? Number)?.toLong()?.takeIf { value !is Double && value !is Float }
                    ?: throw Gql.refused("$failed: $key must be an integer.")
            if (number !in range.first..range.last) {
                throw Gql.refused("$failed: $key is $number, not ${range.first} to ${range.last}.")
            }
            return number.toInt()
        }
    }
}
