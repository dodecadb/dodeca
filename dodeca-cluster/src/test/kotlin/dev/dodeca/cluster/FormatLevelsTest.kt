package dev.dodeca.cluster

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.Test

class FormatLevelsTest {
    @Test
    fun `levels are written as one level or a range, and read back`() {
        for (levels in listOf(FormatLevels(1, 1), FormatLevels(1, 3), FormatLevels(2, 3))) {
            assertThat(FormatLevels.parse(levels.toString())).isEqualTo(levels)
        }
        assertThat(FormatLevels(1, 1).toString()).isEqualTo("1")
        assertThat(FormatLevels(1, 3).toString()).isEqualTo("1-3")
        assertThat(2 in FormatLevels(1, 3)).isTrue()
        assertThat(4 in FormatLevels(1, 3)).isFalse()
    }

    @Test
    fun `a member that reports no levels supports the initial level only`() {
        assertThat(FormatLevels.parse(null)).isEqualTo(FormatLevels(1, 1))
    }

    @Test
    fun `invalid levels are refused`() {
        for (text in listOf("", "x", "1-2-3", "2-1", "0")) {
            assertThatThrownBy { FormatLevels.parse(text) }
                .describedAs(text)
                .isInstanceOf(IllegalArgumentException::class.java)
        }
    }
}
