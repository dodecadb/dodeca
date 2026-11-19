package dev.dodeca.cluster.internal.secondaries

import dev.dodeca.cluster.internal.ClusterMetrics
import dev.dodeca.consensus.BulkSource
import dev.dodeca.consensus.BulkTransfer
import dev.dodeca.consensus.DatabaseRef
import dev.dodeca.consensus.HostPort
import dev.dodeca.consensus.fake.InMemoryConsensus
import java.io.InputStream
import java.nio.file.Path
import java.time.Duration
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.kernel.database.DatabaseIdFactory
import org.neo4j.logging.NullLogProvider

/** The pulls of a secondary, without a database to apply them to. */
class SecondaryPullsTest {
    @TempDir lateinit var root: Path
    private val consensus = InMemoryConsensus()
    private var secondaries: Secondaries? = null

    @AfterEach
    fun close() {
        secondaries?.close()
    }

    private fun secondaries(upstreams: (DatabaseRef) -> List<HostPort>): Secondaries =
        Secondaries(
                transfer =
                    object : BulkTransfer {
                        override fun registerBulkSource(name: String, source: BulkSource) = Unit

                        override fun <T> fetchBulk(
                            address: HostPort,
                            name: String,
                            request: ByteArray,
                            timeout: Duration?,
                            consume: (InputStream) -> T,
                        ): T = error("these tests fetch nothing")
                    },
                upstreams = upstreams,
                primaries = { emptyList() },
                copies =
                    object : SecondaryCopies {
                        override fun copy(source: HostPort, database: DatabaseRef) =
                            error("these tests copy nothing")

                        override fun stage(source: HostPort, database: DatabaseRef) =
                            error("these tests copy nothing")
                    },
                marks = SecondaryMarks(root.resolve("secondaries")),
                control = { error("these tests control no database") },
                pullInterval = Duration.ofMillis(10),
                refusal = { "a secondary" },
                metrics = ClusterMetrics(consensus.newMember(), { emptyList() }),
                logProvider = NullLogProvider.getInstance(),
            )
            .also { secondaries = it }

    @Test
    fun `a secondary whose upstreams could not be read pulls again`() {
        val reads = AtomicInteger()
        val readAgain = CountDownLatch(2)
        val secondaries =
            secondaries(
                upstreams = {
                    readAgain.countDown()
                    check(reads.incrementAndGet() > 1) { "system is not running" }
                    emptyList()
                }
            )
        secondaries.prepare(DatabaseIdFactory.from("films", UUID.randomUUID()))

        secondaries.databaseStart { "films" }

        assertThat(readAgain.await(1, TimeUnit.MINUTES)).isTrue()
    }
}
