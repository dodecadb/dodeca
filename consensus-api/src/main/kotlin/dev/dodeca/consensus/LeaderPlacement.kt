package dev.dodeca.consensus

/** Chooses which member takes over the leadership of a group that this member hands over. */
public fun interface LeaderPlacement {
    /**
     * Orders [candidates], the other voting members of the group of [database], in the order this
     * member tries them. Candidates left out are tried last, in the order given.
     */
    public fun rank(database: DatabaseRef, candidates: List<MemberId>): List<MemberId>

    public companion object {
        /** The candidates in the order of the group's configuration. */
        public val CONFIGURATION_ORDER: LeaderPlacement = LeaderPlacement { _, candidates ->
            candidates
        }
    }
}
