package org.dozendb.cluster.internal

import org.dozendb.cluster.internal.reads.ReadConsistencyException
import org.neo4j.kernel.api.exceptions.Status

/** How up to date a read on a replicated database must be. */
enum class ReadConsistency {
    /** Sees the writes the transaction's bookmarks name, and its own. */
    CAUSAL,

    /** Also sees every write acknowledged anywhere in the cluster before the transaction began. */
    LINEARIZABLE;

    companion object {
        /** The transaction metadata key that picks the consistency of one transaction. */
        const val METADATA_KEY = "dozendb.consistency"

        /** Parses a metadata value, ignoring case; null for anything else. */
        fun parse(value: Any?): ReadConsistency? =
            (value as? String)?.let { text ->
                entries.firstOrNull { it.name.equals(text, ignoreCase = true) }
            }

        /**
         * The consistency a transaction with [metadata] asks for, [default] if it names none;
         * throws a client error for an unknown value.
         */
        fun of(metadata: Map<String, Any?>?, default: ReadConsistency): ReadConsistency {
            val requested = metadata?.get(METADATA_KEY) ?: return default
            return parse(requested)
                ?: throw ReadConsistencyException(
                    Status.Request.Invalid,
                    "Unknown $METADATA_KEY '$requested'; use one of " +
                        entries.joinToString { it.name.lowercase() },
                )
        }
    }
}
