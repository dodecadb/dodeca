package org.dozendb.cluster.core

/** A [MembershipStore] in memory, for tests. */
class InMemoryMembershipStore(@Volatile var record: MembershipRecord? = null) : MembershipStore {
    override fun read(): MembershipRecord? = record

    override fun write(record: MembershipRecord) {
        this.record = record
    }
}
