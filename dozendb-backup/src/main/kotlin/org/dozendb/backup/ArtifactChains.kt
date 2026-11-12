package org.dozendb.backup

import java.io.IOException
import java.nio.file.Path
import org.dozendb.core.store.RestorePoint
import org.neo4j.io.fs.FileSystemAbstraction

/** A backup artifact file and its header. */
class Artifact(val path: Path, val header: ArtifactHeader) {
    /** Whether it holds the transactions right after [previous]'s, in the same history. */
    fun continues(previous: Artifact): Boolean =
        !header.full &&
            !header.empty &&
            !previous.header.empty &&
            header.databaseId == previous.header.databaseId &&
            header.storeId == previous.header.storeId &&
            header.lowestAppendIndex == previous.header.highestAppendIndex + 1 &&
            header.previousChecksum == previous.header.lastChecksum

    override fun toString() = path.fileName.toString()
}

/** A full backup and the differential backups that continue it, in order. */
class ArtifactChain(val artifacts: List<Artifact>) {
    init {
        require(artifacts.isNotEmpty() && artifacts.first().header.full) { "a chain starts full" }
        require(artifacts.zipWithNext().all { (previous, next) -> next.continues(previous) }) {
            "$artifacts is not a chain"
        }
    }

    val full: Artifact
        get() = artifacts.first()

    val differentials: List<Artifact>
        get() = artifacts.drop(1)

    val last: Artifact
        get() = artifacts.last()

    /** The latest users, roles and privileges a backup of the chain holds, if any. */
    val metadataScript: String?
        get() = artifacts.lastOrNull { it.header.metadataScript != null }?.header?.metadataScript

    override fun toString() = artifacts.joinToString(", ")
}

/**
 * Finds the backup artifacts a restore uses: the chain of a full backup and the differential
 * backups that continue it, checked by their headers. Transaction ids and append indexes are the
 * same in the stores DozenDB backs up: Community's record format never splits a transaction.
 */
object ArtifactChains {
    /** The artifacts in [paths], and the files that are not, with why. */
    class Found(val artifacts: List<Artifact>, val unreadable: Map<Path, String>)

    /**
     * Reads the headers of the backup files in [paths]: files, and the `.backup` files in
     * directories. A file named on its own must be a DozenDB backup.
     */
    fun find(paths: Collection<Path>, fs: FileSystemAbstraction): Found {
        val artifacts = mutableListOf<Artifact>()
        val unreadable = mutableMapOf<Path, String>()
        for (path in paths) {
            if (fs.isDirectory(path)) {
                val files =
                    fs.listFiles(path) { it.fileName.toString().endsWith(Artifacts.EXTENSION) }
                for (file in files.sorted()) {
                    if (fs.isDirectory(file)) continue
                    try {
                        artifacts += read(file, fs)
                    } catch (e: IOException) {
                        unreadable[file] = e.message ?: e.toString()
                    }
                }
            } else {
                artifacts += read(path, fs)
            }
        }
        return Found(artifacts.distinctBy { it.path.toAbsolutePath().normalize() }, unreadable)
    }

    private fun read(path: Path, fs: FileSystemAbstraction): Artifact =
        try {
            Artifact(path, fs.openAsInputStream(path).use(Artifacts::readHeader))
        } catch (e: IOException) {
            throw IOException("$path: ${e.message}", e)
        }

    /**
     * The chain of the database [artifacts] belong to, which must be one: by default the chain to
     * the latest transaction of its latest history (the database id and store of its latest
     * backup), or ending with [end]. With [until], the chain that reaches furthest towards it of
     * those whose full backup ends before it.
     */
    fun select(
        artifacts: List<Artifact>,
        until: RestorePoint?,
        end: Artifact? = null,
    ): ArtifactChain {
        val names = artifacts.map { it.header.databaseName }.toSet()
        require(names.size <= 1) { "backups of several databases: $names" }
        val holding = artifacts.filterNot { it.header.empty }
        if (end != null) {
            if (end.header.empty) throw IOException("$end is empty: it holds no transactions")
            val chain = backwards(holding, end)
            if (until != null && !endsBefore(chain.full, until)) {
                throw IOException(
                    "the full backup ${chain.full} holds transactions up to " +
                        "${chain.full.header.highestAppendIndex}, not only those $until"
                )
            }
            return chain
        }
        val latest =
            holding.maxWithOrNull(compareBy({ it.header.time }, { it.header.highestAppendIndex }))
                ?: throw IOException(
                    "no backup with transactions among ${artifacts.ifEmpty { "none" }}"
                )
        val history = holding.filter {
            it.header.databaseId == latest.header.databaseId &&
                it.header.storeId == latest.header.storeId
        }
        if (until == null) {
            // A full backup ending as late as a differential one makes the shorter chain.
            val last =
                history.maxWith(
                    compareBy(
                        { it.header.highestAppendIndex },
                        { it.header.full },
                        { it.header.time },
                    )
                )
            return backwards(history, last)
        }
        val fulls = history.filter { it.header.full && endsBefore(it, until) }
        if (fulls.isEmpty()) {
            throw IOException(
                "no full backup of ${latest.header.databaseName} holds only transactions $until"
            )
        }
        return fulls
            .map { forwards(history, it, until) }
            .maxWith(
                compareBy(
                    { it.last.header.highestAppendIndex },
                    { it.full.header.highestAppendIndex },
                )
            )
    }

    /** Whether every transaction [artifact] holds is restored when restoring [until]. */
    private fun endsBefore(artifact: Artifact, until: RestorePoint): Boolean =
        when (until) {
            is RestorePoint.BeforeTransaction ->
                artifact.header.highestAppendIndex < until.transactionId
            is RestorePoint.BeforeTime -> artifact.header.lastCommitTime!! < until.time
        }

    /** The shortest chain from a full backup to [end]. */
    private fun backwards(artifacts: List<Artifact>, end: Artifact): ArtifactChain {
        // Breadth first, so the first full backup reached makes the shortest chain.
        val reached = mutableMapOf(end to emptyList<Artifact>())
        var frontier = listOf(end)
        var deepest = end
        while (frontier.isNotEmpty()) {
            frontier
                .firstOrNull { it.header.full }
                ?.let {
                    return ArtifactChain(listOf(it) + reached.getValue(it))
                }
            val next = mutableListOf<Artifact>()
            for (artifact in frontier) {
                for (previous in artifacts) {
                    if (previous !in reached && artifact.continues(previous)) {
                        reached[previous] = listOf(artifact) + reached.getValue(artifact)
                        next += previous
                    }
                }
            }
            deepest = frontier.last()
            frontier = next
        }
        throw broken(artifacts, deepest)
    }

    /** The chain from [full] that reaches furthest towards [until]. */
    private fun forwards(
        artifacts: List<Artifact>,
        full: Artifact,
        until: RestorePoint,
    ): ArtifactChain {
        var best = listOf(full)
        fun extend(chain: List<Artifact>) {
            if (chain.last().header.highestAppendIndex > best.last().header.highestAppendIndex) {
                best = chain
            }
            if (!endsBefore(chain.last(), until)) return
            for (next in artifacts) if (next.continues(chain.last())) extend(chain + next)
        }
        extend(listOf(full))
        return ArtifactChain(best)
    }

    private fun broken(artifacts: List<Artifact>, first: Artifact): IOException {
        val before = first.header.lowestAppendIndex - 1
        val endingThere = artifacts.filter { it.header.highestAppendIndex == before }
        val why =
            if (endingThere.isEmpty()) {
                "no backup of ${first.header.databaseName} ends at transaction $before, which " +
                    "$first continues"
            } else {
                "$first continues transaction $before with checksum " +
                    "${first.header.previousChecksum}, but ${endingThere.joinToString()} " +
                    "ended there in another history"
            }
        return IOException("no chain of backups leads to $first: $why")
    }
}
