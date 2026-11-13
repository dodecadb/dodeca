package dev.dodeca.cluster.internal.recreation

import java.util.UUID

/**
 * The store a seeder of a recreated database keeps as its seed (see `dbms.recreateDatabase`): the
 * id of its [lastTransaction], that transaction's [appendIndex] in the Raft log of the database it
 * was recreated from and its [checksum], and its [storeId] (`<creation time>/<random>`).
 */
internal data class Seed(
    val appendIndex: Long,
    val lastTransaction: Long,
    val checksum: Int,
    val storeId: String,
)

/**
 * What a member reports, in `dodeca.recreateSeeds`, about the recreated databases it seeds: for
 * each, by the id it was recreated under, its [Seed], or none if it holds no store of it. A
 * database it has not looked at yet is left out. Entries are `<id>=<append index>:<last
 * transaction>:<checksum>:<store id>` or `<id>=none`, separated by commas.
 */
internal object SeedReports {
    const val KEY = "dodeca.recreateSeeds"
    private const val NONE = "none"

    fun encode(seeds: Map<UUID, Seed?>): String =
        seeds.entries.joinToString(",") { (id, seed) ->
            "$id=" +
                (seed?.let {
                    "${it.appendIndex}:${it.lastTransaction}:${it.checksum}:${it.storeId}"
                } ?: NONE)
        }

    /** The entries of [text]; malformed ones are left out. */
    fun decode(text: String?): Map<UUID, Seed?> =
        text
            .orEmpty()
            .split(',')
            .mapNotNull { entry ->
                val (id, value) =
                    entry.split('=', limit = 2).takeIf { it.size == 2 } ?: return@mapNotNull null
                val uuid = runCatching { UUID.fromString(id) }.getOrNull() ?: return@mapNotNull null
                if (value == NONE) return@mapNotNull uuid to null
                val parts =
                    value.split(':', limit = 4).takeIf { it.size == 4 } ?: return@mapNotNull null
                val appendIndex = parts[0].toLongOrNull() ?: return@mapNotNull null
                val lastTransaction = parts[1].toLongOrNull() ?: return@mapNotNull null
                val checksum = parts[2].toIntOrNull() ?: return@mapNotNull null
                uuid to Seed(appendIndex, lastTransaction, checksum, parts[3])
            }
            .toMap()
}
