package org.dozendb.cluster.internal

import java.io.ByteArrayOutputStream
import java.time.Duration
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.dozendb.cluster.internal.StatusChecks.MemberStatus
import org.dozendb.cluster.internal.StatusChecks.Replication
import org.dozendb.cluster.internal.StatusChecks.Report
import org.dozendb.consensus.DatabaseRef
import org.dozendb.consensus.MemberId
import org.junit.jupiter.api.Test

class StatusCheckProtocolTest {
    private val films = DatabaseRef("films", UUID.randomUUID())
    private val system = DatabaseRef("system", UUID.randomUUID())

    @Test
    fun `requests keep their databases, entries and wait`() {
        val wait = Duration.ofMillis(750)
        val entries =
            listOf(
                StatusCheckProtocol.Entry(films, 42, 3),
                StatusCheckProtocol.Entry(system, -1, -1),
            )

        assertThat(
                StatusCheckProtocol.decode(
                    StatusCheckProtocol.encodeReplicate(listOf(films, system), wait)
                )
            )
            .isEqualTo(StatusCheckProtocol.Request.Replicate(listOf(films, system), wait))
        assertThat(StatusCheckProtocol.decode(StatusCheckProtocol.encodeReport(entries, wait)))
            .isEqualTo(StatusCheckProtocol.Request.Report(entries, wait))
    }

    @Test
    fun `answers keep their order, an unknown leader, and long messages cut short`() {
        val replications = listOf(Replication.Replicated(42, 3), Replication.Failed("no leader"))
        val reports =
            listOf(
                Report.Reported(MemberStatus.APPLYING, MemberId(UUID.randomUUID()), 3),
                Report.Reported(MemberStatus.UNAVAILABLE, null, 4),
                Report.Failed("x".repeat(100_000)),
            )

        val replicated =
            ByteArrayOutputStream().also { StatusCheckProtocol.writeReplications(it, replications) }
        val reported =
            ByteArrayOutputStream().also { StatusCheckProtocol.writeReports(it, reports) }

        assertThat(StatusCheckProtocol.readReplications(replicated.toByteArray().inputStream()))
            .isEqualTo(replications)
        val read = StatusCheckProtocol.readReports(reported.toByteArray().inputStream())
        assertThat(read.take(2)).isEqualTo(reports.take(2))
        assertThat((read[2] as Report.Failed).message)
            .hasSize(8_000)
            .containsOnlyOnce("x".repeat(8_000))
    }
}
