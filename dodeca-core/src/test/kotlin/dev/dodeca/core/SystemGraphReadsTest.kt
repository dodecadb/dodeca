package dev.dodeca.core

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.Label
import org.neo4j.graphdb.NotFoundException
import org.neo4j.graphdb.Transaction

class SystemGraphReadsTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer

    @BeforeEach
    fun start() {
        server = StandaloneServer(root).also { it.start() }
        server
            .database()
            .executeTransactionally("UNWIND ['a', 'b', 'c'] AS name CREATE (:Item {name: name})")
    }

    @AfterEach
    fun stop() {
        server.close()
    }

    private fun names(tx: Transaction): List<String> =
        tx.findNodes(ITEM).use { nodes ->
            nodes.asSequence().map { it.getProperty("name") as String }.sorted().toList()
        }

    @Test
    fun `a read that meets a node deleted meanwhile fails, as Neo4j reads committed data`() {
        val graph = VanishingGraph(server.database(), ITEM) { it.getProperty("name") == "b" }

        assertThatThrownBy { graph.beginTx().use(::names) }
            .isInstanceOf(NotFoundException::class.java)
            .satisfies({ assertThat(SystemGraphReads.vanished(it)).isTrue() })
    }

    @Test
    fun `a read that meets a node deleted meanwhile reads again, without it`() {
        val graph = VanishingGraph(server.database(), ITEM) { it.getProperty("name") == "b" }

        assertThat(SystemGraphReads.read(graph, ::names)).containsExactly("a", "c")
        assertThat(graph.deleted.get()).isEqualTo(1)
    }

    @Test
    fun `a read that meets a node deleted meanwhile reads again in the same transaction, without it`() {
        // As commands read, in the transaction Neo4j runs them in, which reads committed data.
        val graph = VanishingGraph(server.database(), ITEM) { it.getProperty("name") == "b" }

        val names = graph.beginTx().use { tx -> SystemGraphReads.again { names(tx) } }

        assertThat(names).containsExactly("a", "c")
        assertThat(graph.deleted.get()).isEqualTo(1)
    }

    @Test
    fun `a read that meets a deleted node each time fails after the last attempt`() {
        val graph =
            VanishingGraph(server.database(), ITEM, times = SystemGraphReads.ATTEMPTS) { true }
        val reads = AtomicInteger()

        assertThatThrownBy {
                SystemGraphReads.read(graph) { tx ->
                    reads.incrementAndGet()
                    names(tx)
                }
            }
            .isInstanceOf(NotFoundException::class.java)
        assertThat(reads.get()).isEqualTo(SystemGraphReads.ATTEMPTS)
    }

    @Test
    fun `a property that is not there fails the read at once`() {
        val reads = AtomicInteger()

        assertThatThrownBy {
                SystemGraphReads.read(server.database()) { tx ->
                    reads.incrementAndGet()
                    tx.findNodes(ITEM).use { nodes ->
                        nodes.asSequence().map { it.getProperty("age") }.toList()
                    }
                }
            }
            .isInstanceOf(NotFoundException::class.java)
            .satisfies({ assertThat(SystemGraphReads.vanished(it)).isFalse() })
        assertThat(reads.get()).isEqualTo(1)
    }

    @Test
    fun `another failure fails the read at once`() {
        val reads = AtomicInteger()

        assertThatThrownBy {
                SystemGraphReads.read(server.database()) {
                    reads.incrementAndGet()
                    error("broken")
                }
            }
            .hasMessage("broken")
        assertThat(reads.get()).isEqualTo(1)
    }

    private companion object {
        val ITEM: Label = Label.label("Item")
    }
}
