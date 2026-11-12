package org.dozendb.cluster.internal

import org.dozendb.cluster.internal.recreation.SeedChecksums
import org.dozendb.cluster.internal.replication.EntryTermProtocol
import org.dozendb.cluster.internal.replication.TransactionEnvelope
import org.dozendb.cluster.internal.secondaries.PullProtocol
import org.dozendb.cluster.internal.secondaries.ReadBarrierProtocol
import org.dozendb.cluster.internal.storecopy.StoreCopySource
import org.dozendb.consensus.MemberConnection

/**
 * The rows of `dbms.cluster.protocols`: one per open connection between this member and another,
 * and per protocol it carried, with the version `FORMATS.md` lists for it. `raft` is the consensus
 * library's protocol, at the version of the replicated transactions it carries; a bulk transfer
 * rides on `bulk`. `modifierProtocols` is `TLS` for an encrypted connection, else empty.
 */
internal object ClusterProtocols {
    /** The protocols, in the order rows of one connection list them, with their versions. */
    val VERSIONS: Map<String, Long> =
        linkedMapOf(
            RAFT to TransactionEnvelope.FORMAT.toLong(),
            // ProbeCodec.FORMAT_VERSION and BulkService's, in the consensus module.
            PROBE to 2L,
            BULK to 1L,
            StoreCopySource.NAME to StoreCopySource.REQUEST_VERSION.toLong(),
            PullProtocol.NAME to PullProtocol.VERSION.toLong(),
            ReadBarrierProtocol.NAME to ReadBarrierProtocol.VERSION.toLong(),
            StatusCheckProtocol.NAME to StatusCheckProtocol.VERSION.toLong(),
            QuarantineProtocol.NAME to QuarantineProtocol.VERSION.toLong(),
            EntryTermProtocol.NAME to EntryTermProtocol.VERSION.toLong(),
            SeedChecksums.NAME to SeedChecksums.VERSION.toLong(),
        )

    /** A row, in Neo4j Enterprise's columns. */
    data class Row(
        val orientation: String,
        val remoteAddress: String,
        val applicationProtocol: String,
        val applicationProtocolVersion: Long?,
        val modifierProtocols: String,
    )

    /** The rows of [connections]: inbound first, then by address, then in protocol order. */
    fun rows(connections: List<MemberConnection>): List<Row> =
        connections
            .sortedWith(compareBy({ !it.inbound }, { it.remoteAddress.toString() }))
            .flatMap { connection ->
                connection.protocols.sortedBy(::rank).map { protocol ->
                    Row(
                        if (connection.inbound) INBOUND else OUTBOUND,
                        connection.remoteAddress.toString(),
                        protocol,
                        VERSIONS[protocol],
                        if (connection.tls) TLS else "",
                    )
                }
            }
            .distinct()

    private fun rank(protocol: String): Int =
        VERSIONS.keys.indexOf(protocol).let { if (it < 0) VERSIONS.size else it }

    const val RAFT = "raft"
    const val PROBE = "probe"
    const val BULK = "bulk"
    private const val INBOUND = "inbound"
    private const val OUTBOUND = "outbound"
    private const val TLS = "TLS"
}
