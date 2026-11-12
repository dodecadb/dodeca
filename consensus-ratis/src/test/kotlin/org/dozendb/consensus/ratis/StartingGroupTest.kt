package org.dozendb.consensus.ratis

import java.lang.reflect.Proxy
import org.apache.ratis.server.DivisionInfo
import org.apache.ratis.server.RaftServer
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * A group that Ratis is starting: it counts as running once its log is loaded, a moment before it
 * creates the updater that applies the log, whose index it then cannot tell.
 */
class StartingGroupTest {
    /** A division whose info answers the last applied index with [applied]. */
    private fun division(applied: () -> Long): RaftServer.Division {
        val info =
            Proxy.newProxyInstance(javaClass.classLoader, arrayOf(DivisionInfo::class.java)) {
                _,
                method,
                _ ->
                when (method.name) {
                    "getLastAppliedIndex" -> applied()
                    else -> throw UnsupportedOperationException(method.name)
                }
            } as DivisionInfo
        return Proxy.newProxyInstance(
            javaClass.classLoader,
            arrayOf(RaftServer.Division::class.java),
        ) { _, method, _ ->
            when (method.name) {
                "getInfo" -> info
                else -> throw UnsupportedOperationException(method.name)
            }
        } as RaftServer.Division
    }

    @Test
    fun `a group without its state machine updater yet has no applied index`() {
        val starting = division {
            throw IllegalStateException("s1@group-1: stateMachineUpdater is uninitialized.")
        }

        assertThat(appliedIndex(starting)).isNull()
        assertThat(appliedIndex(division { 42 })).isEqualTo(42)
    }
}
