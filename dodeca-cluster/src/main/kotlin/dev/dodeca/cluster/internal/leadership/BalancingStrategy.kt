package dev.dodeca.cluster.internal.leadership

/** The values of `dbms.cluster.raft.leader_transfer.balancing_strategy`, as in Neo4j Enterprise. */
enum class BalancingStrategy {
    /** Leaders stay where elections put them, except off members where a database is read-only. */
    NO_BALANCING,

    /** Every member leads as many databases as the others, give or take one. */
    EQUAL_BALANCING,
}
