package dev.dodeca.core.edition

/** The operations of `dbms.unquarantineDatabase`, as Neo4j Enterprise documents them. */
enum class UnquarantineOperation(val label: String) {
    /** Leaves the store and the cluster state as they are. */
    KEEP_STATE_KEEP_STORE("keepStateKeepStore"),

    /** Joins as a new member, clearing the cluster state but keeping the store. */
    REPLACE_STATE_KEEP_STORE("replaceStateKeepStore"),

    /** Joins as a new member, clearing both the cluster state and the store. */
    REPLACE_STATE_REPLACE_STORE("replaceStateReplaceStore");

    companion object {
        /**
         * The operation called [label].
         *
         * @throws IllegalArgumentException if none is.
         */
        fun of(label: String): UnquarantineOperation =
            entries.firstOrNull { it.label == label }
                ?: throw IllegalArgumentException(
                    "Unknown operation '$label': the operations are " +
                        entries.dropLast(1).joinToString { it.label } +
                        " and ${entries.last().label}."
                )
    }
}
