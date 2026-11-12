package org.dozendb.cluster.internal.recreation

import org.dozendb.core.Gql
import org.neo4j.configuration.Config
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.storageengine.api.StorageEngineFactory

/**
 * The options of `dbms.recreateDatabase`, as Neo4j Enterprise documents them.
 *
 * @property seedUri the backup or dump every copy's store is replaced by; with [seedingServers],
 *   the fallback when no seeder holds a store.
 * @property seedingServers the servers whose stores are synchronised to the most up-to-date of
 *   them, by id; empty for every server hosting a copy that is enabled; null if not given.
 * @property primaries the new number of primaries; null to keep the topology.
 * @property secondaries the new number of secondaries; null to keep the topology, 0 when only
 *   [primaries] is given.
 * @property clearData whether it is recreated empty, without data or schema, instead of from a
 *   seed.
 * @property storeFormat the format of its empty store, only with [clearData]; null for the one it
 *   was created in.
 */
internal data class RecreateOptions(
    val seedUri: String?,
    val seedingServers: List<String>?,
    val primaries: Int?,
    val secondaries: Int?,
    val clearData: Boolean = false,
    val storeFormat: String? = null,
) {
    companion object {
        const val SEED_URI = "seedURI"
        const val SEEDING_SERVERS = "seedingServers"
        const val CLEAR_DATA = "clearData"
        const val STORE_FORMAT = "storeFormat"
        const val PRIMARIES = "primaries"
        const val SECONDARIES = "secondaries"
        private val KEYS =
            listOf(SEED_URI, SEEDING_SERVERS, CLEAR_DATA, STORE_FORMAT, PRIMARIES, SECONDARIES)

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
            val clearData =
                options[CLEAR_DATA]?.let {
                    it as? Boolean ?: throw Gql.refused("$failed: $CLEAR_DATA must be a boolean.")
                } ?: false
            val storeFormat =
                options[STORE_FORMAT]?.let {
                    it as? String ?: throw Gql.refused("$failed: $STORE_FORMAT must be a string.")
                }
            if (clearData && (seedUri != null || seedingServers != null)) {
                throw Gql.refused(
                    "$failed: $CLEAR_DATA recreates it empty, without $SEED_URI or " +
                        "$SEEDING_SERVERS."
                )
            }
            if (!clearData && seedUri == null && seedingServers == null) {
                throw Gql.refused(
                    "$failed: give $SEEDING_SERVERS, the servers whose stores to recreate it " +
                        "from, $SEED_URI, a backup or dump to recreate it from, or $CLEAR_DATA, " +
                        "to recreate it empty."
                )
            }
            if (storeFormat != null) {
                if (!clearData) {
                    throw Gql.refused(
                        "$failed: $STORE_FORMAT needs $CLEAR_DATA: a database recreated from a " +
                            "seed keeps the seed's store format."
                    )
                }
                checkFormat(storeFormat, failed)
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
                clearData,
                storeFormat,
            )
        }

        /**
         * Refuses [format] unless a storage engine stores it, as Neo4j checks the store format of
         * `CREATE DATABASE`.
         */
        private fun checkFormat(format: String, failed: String) {
            val stored = runCatching {
                StorageEngineFactory.selectStorageEngine(
                    Config.newBuilder().set(GraphDatabaseSettings.db_format, format).build()
                )
            }
                .isSuccess
            if (stored) return
            val formats =
                StorageEngineFactory.allAvailableStorageEngines()
                    .flatMap { it.supportedFormats(false) }
                    .distinct()
                    .sorted()
            throw Gql.refused(
                "$failed: '$format' is no store format. The formats are ${formats.joinToString()}."
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
