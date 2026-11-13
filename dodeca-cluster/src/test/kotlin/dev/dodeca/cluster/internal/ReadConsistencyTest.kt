package dev.dodeca.cluster.internal

import dev.dodeca.cluster.internal.reads.ReadConsistencyException
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test
import org.neo4j.kernel.api.exceptions.Status

class ReadConsistencyTest {
    private fun of(
        metadata: Map<String, Any?>?,
        default: ReadConsistency = ReadConsistency.CAUSAL,
    ) = ReadConsistency.of(metadata, default)

    @Test
    fun `the metadata key picks the consistency, ignoring case`() {
        assertThat(of(null)).isEqualTo(ReadConsistency.CAUSAL)
        assertThat(of(mapOf("other" to 1))).isEqualTo(ReadConsistency.CAUSAL)
        assertThat(of(mapOf(ReadConsistency.METADATA_KEY to "Linearizable")))
            .isEqualTo(ReadConsistency.LINEARIZABLE)
        assertThat(
                of(mapOf(ReadConsistency.METADATA_KEY to "causal"), ReadConsistency.LINEARIZABLE)
            )
            .isEqualTo(ReadConsistency.CAUSAL)
        assertThat(of(emptyMap(), ReadConsistency.LINEARIZABLE))
            .isEqualTo(ReadConsistency.LINEARIZABLE)
    }

    @Test
    fun `an unknown consistency is a client error`() {
        assertThatThrownBy { of(mapOf(ReadConsistency.METADATA_KEY to "strong")) }
            .isInstanceOfSatisfying(ReadConsistencyException::class.java) {
                assertThat(it.status()).isEqualTo(Status.Request.Invalid)
            }
            .hasMessageContaining("causal, linearizable")
    }
}
