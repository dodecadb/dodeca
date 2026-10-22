package org.dozendb.core.security

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.SoftAssertions.assertSoftly
import org.junit.jupiter.api.Test
import org.neo4j.internal.kernel.api.security.PrivilegeAction

/** DozenDB's reading of Neo4j's privilege actions, which the policy relies on. */
class PrivilegeActionsTest {
    @Test
    fun `every action of Neo4j is placed`() {
        assertThat(ReferencePolicy.COVERS.keys)
            .containsExactlyInAnyOrder(*PrivilegeAction.entries.toTypedArray())
    }

    @Test
    fun `Neo4j's satisfies agrees with the docs on single actions`() {
        assertSoftly { softly ->
            for (action in PrivilegeAction.entries) {
                for (single in ReferencePolicy.SINGLE) {
                    softly
                        .assertThat(action.satisfies(single))
                        .describedAs("$action satisfies $single")
                        .isEqualTo(single in ReferencePolicy.COVERS.getValue(action))
                }
            }
        }
    }

    @Test
    fun `an action satisfies a group only if it covers all of it`() {
        for (action in PrivilegeAction.entries) {
            for (group in PrivilegeAction.entries.filter { it !in ReferencePolicy.SINGLE }) {
                if (action.satisfies(group)) {
                    assertThat(ReferencePolicy.COVERS.getValue(action))
                        .describedAs("$action satisfies $group")
                        .containsAll(ReferencePolicy.COVERS.getValue(group))
                }
            }
        }
    }
}
