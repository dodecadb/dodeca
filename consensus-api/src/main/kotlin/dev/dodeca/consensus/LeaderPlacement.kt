package dev.dodeca.consensus

/** Chooses which member takes over the leadership of a group that this member hands over. */
public fun interface LeaderPlacement {
    /**
     * Orders [candidates], the other voting members of the group of [database], in the order this
     * member tries them. Candidates left out are tried last, in the order given.
     */
    public fun rank(database: DatabaseRef, candidates: List<MemberId>): List<MemberId>

    /**
     * Orders the candidates of each group of [groups], whose leaderships this member hands over
     * together, as [rank] does, each as if those before it had gone to their first candidate: so
     * that they spread as they would one after the other. By default each is ranked on its own.
     */
    public fun rankTogether(
        groups: Map<DatabaseRef, List<MemberId>>
    ): Map<DatabaseRef, List<MemberId>> = groups.mapValues { (database, candidates) ->
        rank(database, candidates)
    }

    public companion object {
        /** The candidates in the order of the group's configuration. */
        public val CONFIGURATION_ORDER: LeaderPlacement = LeaderPlacement { _, candidates ->
            candidates
        }
    }
}
