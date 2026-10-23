package org.dozendb.core.logging

import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

class QueryTextTest {
    @Test
    fun `masks string literals, whatever the query's mistake`() {
        assertThat(QueryText.maskLiterals("CREATE USER bob SET PASSWRD 'bob-secret-1' CHANGE"))
            .isEqualTo("CREATE USER bob SET PASSWRD '******' CHANGE")
        assertThat(QueryText.maskLiterals("""RETURN "a\"b", 'c\'d', 'e\\'"""))
            .isEqualTo("""RETURN "******", '******', '******'""")
        assertThat(QueryText.maskLiterals("RETURN 'unclosed secret")).isEqualTo("RETURN '******")
        assertThat(QueryText.maskLiterals("RETURN '' AS empty")).isEqualTo("RETURN '' AS empty")
    }

    @Test
    fun `leaves names and comments alone`() {
        val text = "MATCH (`it's`) // don't\nRETURN /* a 'b' */ 1"
        assertThat(QueryText.maskLiterals(text)).isEqualTo(text)
    }

    @Test
    fun `scrubs what a message quotes of the query, and hidden literals anywhere`() {
        val raw = "CREATE USER bob\nSET PASSWRD 'bob-secret-1'"
        val shown = QueryText.maskLiterals(raw)

        assertThat(
                QueryText.scrub(
                    "Invalid input 'PASSWRD' (line 2, column 5) \"SET PASSWRD 'bob-secret-1'\"",
                    raw,
                    shown,
                )
            )
            .isEqualTo("Invalid input 'PASSWRD' (line 2, column 5) \"SET PASSWRD '******'\"")
        assertThat(QueryText.scrub("Unexpected 'bob-secret-1'.", raw, shown))
            .isEqualTo("Unexpected '******'.")
        assertThat(QueryText.scrub("\"$raw\" failed", raw, shown)).isEqualTo("\"$shown\" failed")
        // Literals the log shows stay.
        assertThat(QueryText.scrub("No 'films'.", "USE 'films'", "USE 'films'"))
            .isEqualTo("No 'films'.")
    }
}
