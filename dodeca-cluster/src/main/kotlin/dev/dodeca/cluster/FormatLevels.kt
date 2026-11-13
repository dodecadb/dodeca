package dev.dodeca.cluster

/**
 * The format levels a Dodeca version supports. A format level names the formats members send each
 * other and keep, such as entries of the Raft log. A member reads every level from [oldest] to
 * [newest], and writes the cluster's level, which rises only when an administrator finishes an
 * upgrade: until then, members can return to the previous Dodeca version.
 */
data class FormatLevels(val oldest: Int, val newest: Int) {
    init {
        require(oldest in INITIAL..newest) { "invalid format levels $oldest-$newest" }
    }

    operator fun contains(level: Int): Boolean = level in oldest..newest

    /** `1` for one level, `1-2` for several; [parse] reads it. */
    override fun toString(): String = if (oldest == newest) "$oldest" else "$oldest-$newest"

    companion object {
        /** The level of a cluster that never raised it: the formats of the first version. */
        const val INITIAL = 1

        /** The levels of this Dodeca version. */
        val SUPPORTED = FormatLevels(INITIAL, 1)

        /**
         * The levels in [text], as [toString] writes them. A member that reports none runs a
         * version that knows only the [INITIAL] level.
         */
        fun parse(text: String?): FormatLevels {
            if (text == null) return FormatLevels(INITIAL, INITIAL)
            val bounds = text.split('-').map { it.trim().toIntOrNull() }
            require(bounds.size in 1..2 && bounds.none { it == null }) {
                "invalid format levels '$text'"
            }
            return FormatLevels(bounds.first()!!, bounds.last()!!)
        }
    }
}

/** Where the cluster records its format level: the replicated system graph. */
interface FormatLevelStore {
    /** The cluster's format level, as this member's system graph holds it. */
    fun formatLevel(): Int

    /** Records [level] as the cluster's format level; only the leader of `system` can. */
    fun recordFormatLevel(level: Int)
}

/** A member that cannot take part in the cluster with the versions it runs. */
class IncompatibleVersionException(message: String) : RuntimeException(message)

/** An upgrade that cannot be finished, and why. */
class UpgradeException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
