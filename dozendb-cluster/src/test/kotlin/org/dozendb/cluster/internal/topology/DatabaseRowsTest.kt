package org.dozendb.cluster.internal.topology

import java.util.Optional
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.consensus.MemberId
import org.dozendb.core.edition.DatabaseState
import org.dozendb.core.servers.CopyMode
import org.junit.jupiter.api.Test
import org.neo4j.configuration.helpers.SocketAddress
import org.neo4j.dbms.database.DatabaseDetails
import org.neo4j.dbms.identity.ServerId
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel.DatabaseAccess
import org.neo4j.kernel.database.DatabaseIdFactory

class DatabaseRowsTest {
    private val a = MemberId(UUID(0, 1))
    private val b = MemberId(UUID(0, 2))
    private val c = MemberId(UUID(0, 3))
    private val d = MemberId(UUID(0, 4))
    private val id = DatabaseIdFactory.from("films", UUID.randomUUID())

    /** Neo4j Community's row for this member, [a]. */
    private val own =
        DatabaseDetails(
            Optional.of(ServerId(a.uuid)),
            DatabaseAccess.READ_WRITE,
            Optional.of(SocketAddress("a", 7687)),
            Optional.of(DatabaseDetails.ROLE_PRIMARY),
            true,
            "online",
            "",
            Optional.empty(),
            Optional.of(0L),
            id,
            DatabaseDetails.TYPE_STANDARD,
            emptyMap(),
            Optional.empty(),
            Optional.empty(),
            1,
            0,
        )

    private fun copy(
        member: MemberId,
        status: DatabaseState.Status? = DatabaseState.Status.ONLINE,
        answering: Boolean = true,
        readOnly: Boolean = false,
        lastCommitted: Long? = null,
        failure: String? = null,
        mode: CopyMode = CopyMode.PRIMARY,
        paused: Boolean = false,
    ) =
        Copy(
            member,
            answering,
            SocketAddress("host${member.uuid.leastSignificantBits}", 7687),
            status?.let {
                DatabaseState("films", id.databaseId().uuid(), it, failure, paused = paused)
            },
            readOnly,
            lastCommitted,
            mode,
        )

    private fun rows(
        copies: List<Copy>,
        leader: MemberId? = b,
        txInfo: Boolean = false,
        placed: Set<MemberId> = emptySet(),
    ) =
        DatabaseRows.rows(own, copies, leader, txInfo, placed).associateBy {
            MemberId(it.serverId().orElseThrow().uuid())
        }

    @Test
    fun `a copy in quarantine is quarantined with its message, of an unknown role, no writer`() {
        val message = "By neo4j at 2026-11-10T09:08:07.006Z: Inspecting its files"
        val rows =
            rows(
                listOf(
                    copy(a),
                    copy(b, DatabaseState.Status.QUARANTINED, failure = message),
                    copy(c),
                ),
                leader = b,
            )

        val quarantined = rows.getValue(b)
        assertThat(quarantined.status()).isEqualTo("quarantined")
        assertThat(quarantined.statusMessage()).isEqualTo(message)
        assertThat(quarantined.role()).contains("unknown")
        assertThat(quarantined.writer()).isFalse()
        assertThat(rows.getValue(a).actualPrimariesCount()).isEqualTo(2)
    }

    @Test
    fun `a copy on a server the database moves off is deallocating, and may still be the writer`() {
        val rows = rows(listOf(copy(a), copy(b), copy(c)), leader = c, placed = setOf(a, b))

        assertThat(rows.getValue(c).status()).isEqualTo("deallocating")
        assertThat(rows.getValue(c).writer()).isTrue()
        assertThat(rows.getValue(a).status()).isEqualTo("online")
        assertThat(rows.values.map { it.actualPrimariesCount() }).containsOnly(3)
    }

    @Test
    fun `a copy whose Raft group has yet to start shows starting, and is not counted as online`() {
        val rows =
            rows(listOf(copy(a), copy(b, DatabaseState.Status.STARTING), copy(c)), leader = a)

        assertThat(rows.getValue(b).status()).isEqualTo("starting")
        assertThat(rows.values.map { it.actualPrimariesCount() }).containsOnly(2)
    }

    @Test
    fun `a server that copies the database in shows store copying, one without it initial`() {
        val rows =
            rows(
                listOf(copy(a), copy(b, DatabaseState.Status.COPYING), copy(c, status = null)),
                leader = a,
                placed = setOf(a, b, c),
            )

        assertThat(rows.getValue(b).status()).isEqualTo("store copying")
        assertThat(rows.getValue(c).status()).isEqualTo("initial")
        assertThat(rows.filterValues { it.writer() }.keys).containsExactly(a)
        assertThat(rows.values.map { it.actualPrimariesCount() }).containsOnly(1)
    }

    @Test
    fun `each member has a row with its address, and only the leader's is the writer`() {
        val rows = rows(listOf(copy(a), copy(b), copy(c)))

        assertThat(rows.keys).containsExactlyInAnyOrder(a, b, c)
        assertThat(rows.filterValues { it.writer() }.keys).containsExactly(b)
        assertThat(rows.getValue(c).boltAddress()).contains(SocketAddress("host3", 7687))
        assertThat(rows.values.map { it.status() }).containsOnly("online")
        assertThat(rows.values.map { it.actualPrimariesCount() }).containsOnly(3)
        assertThat(rows.values.map { it.namedDatabaseId() }).containsOnly(id)
    }

    @Test
    fun `a database read-only on its leader, or offline there, has no writer`() {
        val readOnly = rows(listOf(copy(a), copy(b, readOnly = true), copy(c)))
        val offline = rows(listOf(copy(a), copy(b, DatabaseState.Status.OFFLINE), copy(c)))

        assertThat(readOnly.values).noneMatch { it.writer() }
        assertThat(readOnly.getValue(b).databaseAccess()).isEqualTo(DatabaseAccess.READ_ONLY)
        assertThat(offline.values).noneMatch { it.writer() }
        assertThat(offline.getValue(b).status()).isEqualTo("offline")
        assertThat(offline.values.map { it.actualPrimariesCount() }).containsOnly(2)
    }

    @Test
    fun `a secondary has its role, is never the writer, and is counted apart from primaries`() {
        val rows =
            rows(
                listOf(
                    copy(a),
                    copy(b),
                    copy(c, mode = CopyMode.SECONDARY),
                    copy(d, DatabaseState.Status.COPYING, mode = CopyMode.SECONDARY),
                ),
                leader = c,
                placed = setOf(a, b, c, d),
            )

        assertThat(rows.mapValues { it.value.role().orElseThrow() })
            .containsExactlyInAnyOrderEntriesOf(
                mapOf(a to "primary", b to "primary", c to "secondary", d to "secondary")
            )
        assertThat(rows.values).noneMatch { it.writer() }
        assertThat(rows.getValue(c).status()).isEqualTo("online")
        assertThat(rows.getValue(d).status()).isEqualTo("store copying")
        assertThat(rows.values.map { it.actualPrimariesCount() }).containsOnly(2)
        assertThat(rows.values.map { it.actualSecondariesCount() }).containsOnly(1)
    }

    @Test
    fun `a paused secondary says so`() {
        val rows =
            rows(listOf(copy(a), copy(b, mode = CopyMode.SECONDARY, paused = true)), leader = a)

        assertThat(rows.getValue(b).status()).isEqualTo("online")
        assertThat(rows.getValue(b).statusMessage()).isEqualTo("Secondary replication is paused")
        assertThat(rows.getValue(a).statusMessage()).isEmpty()
    }

    @Test
    fun `a secondary on a server the database no longer has one on is deallocating`() {
        val rows =
            rows(
                listOf(copy(a), copy(b), copy(c, mode = CopyMode.SECONDARY)),
                leader = a,
                placed = setOf(a, b),
            )

        assertThat(rows.getValue(c).status()).isEqualTo("deallocating")
        assertThat(rows.getValue(c).role()).contains("secondary")
    }

    @Test
    fun `a member that does not answer has the role and status unknown, and no address`() {
        val rows =
            rows(listOf(copy(a), copy(b), copy(c, answering = false, mode = CopyMode.SECONDARY)))

        val c = rows.getValue(c)
        assertThat(c.boltAddress()).isEmpty()
        assertThat(c.role()).contains("unknown")
        assertThat(c.status()).isEqualTo(DatabaseDetails.STATUS_UNKNOWN)
        assertThat(c.statusMessage()).isEqualTo(DatabaseDetails.STATUS_MESSAGE_UNKNOWN)
        assertThat(c.writer()).isFalse()
    }

    @Test
    fun `a copy that failed is offline and says why, one not reported yet is offline`() {
        val rows =
            rows(
                listOf(
                    copy(a),
                    copy(b, DatabaseState.Status.FAILED, failure = "disk on fire"),
                    copy(c, status = null),
                ),
                leader = a,
            )

        assertThat(rows.getValue(b).status()).isEqualTo("offline")
        assertThat(rows.getValue(b).statusMessage()).isEqualTo("disk on fire")
        assertThat(rows.getValue(c).status()).isEqualTo("offline")
        assertThat(rows.getValue(c).statusMessage()).isEmpty()
    }

    @Test
    fun `with transaction ids each copy shows its last one and how far it is behind the newest`() {
        val copies =
            listOf(
                copy(a, lastCommitted = 90),
                copy(b, lastCommitted = 100),
                // What it reported before it stopped answering.
                copy(c, answering = false, lastCommitted = 120),
            )

        val rows = rows(copies, txInfo = true)
        val without = rows(copies)

        assertThat(rows.getValue(a).lastCommittedTxId()).contains(90L)
        assertThat(rows.getValue(a).txCommitLag()).contains(-10L)
        assertThat(rows.getValue(b).txCommitLag()).contains(0L)
        assertThat(rows.getValue(c).lastCommittedTxId()).isEmpty()
        assertThat(rows.getValue(c).txCommitLag()).isEmpty()
        assertThat(without.getValue(a).lastCommittedTxId()).isEmpty()
    }
}
