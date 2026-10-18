package org.dozendb.cluster.ratis

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class PublicApiTest {
    /** Everything other modules may use. The rest of this module is `internal`. */
    private val publicTypes =
        listOf(RatisConsensusLog::class.java, RatisConsensusConfig::class.java)

    @Test
    fun `the public API exposes no Ratis types`() {
        val leaks = publicTypes.flatMap { type ->
            // Synthetic members are compiler-generated accessors, not API.
            val methods = type.methods.filterNot { it.isSynthetic }
            val constructors = type.constructors.filterNot { it.isSynthetic }
            val signatures =
                methods.flatMap { listOf(it.genericReturnType) + it.genericParameterTypes } +
                    constructors.flatMap { it.genericParameterTypes.toList() }
            signatures
                .map { it.typeName }
                .filter { "org.apache.ratis" in it }
                .map { "${type.simpleName}: $it" }
        }

        assertThat(leaks).isEmpty()
    }
}
