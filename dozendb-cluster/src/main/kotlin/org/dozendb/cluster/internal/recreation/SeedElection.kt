package org.dozendb.cluster.internal.recreation

import java.util.UUID
import org.dozendb.consensus.MemberId
import org.neo4j.kernel.database.NamedDatabaseId

/** What the system leader learns of the seeds that the seeders of recreated databases keep. */
internal interface RecreationSeeds {
    /**
     * What [member] last reported of the seeds it keeps, by the ids of the databases they seed;
     * null if it has not reported.
     */
    fun reported(member: MemberId): Map<UUID, Seed?>?

    /** Asks the members to report again soon. */
    fun refresh()

    /**
     * The checksums of the transactions at [appendIndexes] in the seed that [member] keeps of
     * [database], from its logs; null for one its logs do not hold.
     *
     * @throws Exception if the member cannot tell, as while it does not answer.
     */
    fun checksums(
        member: MemberId,
        database: NamedDatabaseId,
        appendIndexes: List<Long>,
    ): Map<Long, Int?>
}

/** Whom the system leader names the creator of a recreated database, or why none yet. */
internal sealed interface Election {
    /** Not yet: [message] says what it waits for. */
    data class Waiting(val message: String) : Election

    /**
     * The seeder whose store is the latest, which creates the database from it; its last
     * transaction has [appendIndex].
     */
    data class Creator(val member: MemberId, val appendIndex: Long) : Election

    /** No seeder holds a store, as [message] says. */
    data class NoStore(val message: String) : Election

    /** The seeders' stores do not make one database, as [message] says. */
    data class Failed(val message: String) : Election
}

/**
 * How the system leader chooses the creator of a database that `dbms.recreateDatabase` recreated
 * from the stores of its seeders, as Neo4j Enterprise documents it: once every seeder reported its
 * seed, the seeder whose last transaction has the latest append index, then the lowest id. The
 * seeds must have the same store id, and each other seeder's last transaction the same checksum as
 * the creator's transaction at its append index, which the creator reads from its logs; one its
 * logs no longer hold cannot be checked, and passes. Failures carry Neo4j Enterprise's status
 * messages.
 */
internal object SeedElection {
    fun elect(
        database: NamedDatabaseId,
        seeders: Collection<MemberId>,
        seeds: RecreationSeeds,
    ): Election {
        val id = database.databaseId().uuid()
        val sorted = seeders.distinct().sorted()
        val reports = sorted.associateWith { seeds.reported(it) }
        val silent = sorted.filter { reports[it]?.containsKey(id) != true }
        if (silent.isNotEmpty()) {
            return Election.Waiting(
                if (silent.size == 1) "Waiting for seeder ${silent[0]} to report its store."
                else "Waiting for seeders ${silent.joinToString()} to report their stores."
            )
        }
        val held = sorted.mapNotNull { member -> reports[member]?.get(id)?.let { member to it } }
        if (held.isEmpty()) {
            return Election.NoStore(
                "No store found on any of the seeders ${sorted.joinToString()}."
            )
        }
        val (first, firstSeed) = held.first()
        held
            .firstOrNull { it.second.storeId != firstSeed.storeId }
            ?.let { (other, _) ->
                return Election.Failed(
                    "Seeders $first and $other have incompatible storeIds. All seeders must " +
                        "have compatible storeIds."
                )
            }
        val (creator, latest) =
            held
                .sortedWith(
                    compareByDescending<Pair<MemberId, Seed>> { it.second.appendIndex }
                        .thenBy { it.first }
                )
                .first()
        val others = held.filter { it.first != creator }
        val earlier = others.filter { it.second.appendIndex < latest.appendIndex }
        val known: Map<Long, Int?> =
            if (earlier.isEmpty()) {
                emptyMap()
            } else {
                try {
                    seeds.checksums(
                        creator,
                        database,
                        earlier.map { it.second.appendIndex }.distinct().sorted(),
                    )
                } catch (e: InterruptedException) {
                    throw e
                } catch (e: Exception) {
                    return Election.Waiting(
                        "Waiting for seeder $creator, whose store is the latest, to tell the " +
                            "checksums of its transactions: ${e.message}"
                    )
                }
            }
        for ((member, seed) in others) {
            val expected =
                if (seed.appendIndex == latest.appendIndex) latest.checksum
                else known[seed.appendIndex] ?: continue
            if (seed.checksum != expected) {
                return Election.Failed(
                    "Seeders $creator and $member have different checksums for transaction " +
                        "${seed.lastTransaction}. All seeders must have the same checksum for " +
                        "the same append index."
                )
            }
        }
        return Election.Creator(creator, latest.appendIndex)
    }
}
