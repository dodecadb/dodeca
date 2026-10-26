package org.dozendb.cluster.internal

import org.dozendb.cluster.MemberMetadata
import org.dozendb.core.servers.ServerOptions
import org.neo4j.dbms.systemgraph.InstanceModeConstraint

/**
 * The options of a member's `initial.server.*` settings as it reports them to the others (see
 * [MemberMetadata.INITIAL_SERVER_TAGS] and the keys after it), which its server record takes when
 * it is enabled.
 */
internal object InitialServerOptions {
    /** What a member reports of [options]. */
    fun report(options: ServerOptions): Map<String, String> =
        mapOf(
            MemberMetadata.INITIAL_SERVER_TAGS to options.tags.joinToString(","),
            MemberMetadata.INITIAL_SERVER_MODE_CONSTRAINT to options.modeConstraint.name,
            MemberMetadata.INITIAL_SERVER_ALLOWED_DATABASES to
                options.allowedDatabases.joinToString(","),
            MemberMetadata.INITIAL_SERVER_DENIED_DATABASES to
                options.deniedDatabases.joinToString(","),
        )

    /** The options a member reported in [metadata], or null if it did not report them. */
    fun reported(metadata: Map<String, String>): ServerOptions? {
        val tags = metadata[MemberMetadata.INITIAL_SERVER_TAGS] ?: return null
        return ServerOptions(
            MemberMetadata.splitList(tags),
            metadata[MemberMetadata.INITIAL_SERVER_MODE_CONSTRAINT]?.let { mode ->
                InstanceModeConstraint.entries.firstOrNull { it.name == mode }
            } ?: InstanceModeConstraint.NONE,
            metadata[MemberMetadata.INITIAL_SERVER_ALLOWED_DATABASES]
                ?.let(MemberMetadata::splitList)
                .orEmpty(),
            metadata[MemberMetadata.INITIAL_SERVER_DENIED_DATABASES]
                ?.let(MemberMetadata::splitList)
                .orEmpty(),
        )
    }
}
