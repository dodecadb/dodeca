package org.dozendb.core.servers

import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test
import org.neo4j.dbms.systemgraph.InstanceModeConstraint
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.VirtualValues

class ServerOptionsInputTest {
    private fun options(vararg entries: Pair<String, AnyValue>): MapValue =
        VirtualValues.map(
            entries.map { it.first }.toTypedArray(),
            entries.map { it.second }.toTypedArray(),
        )

    private fun list(vararg values: String): AnyValue =
        VirtualValues.list(*values.map { Values.stringValue(it) }.toTypedArray())

    private fun parse(given: MapValue, defaults: ServerOptions? = ServerOptions()) =
        ServerOptionsInput.parse(given, defaults, "Could not alter server 'a'")

    private fun failure(given: MapValue, defaults: ServerOptions? = ServerOptions()): String =
        catchThrowable {
            parse(given, defaults)
        }
        .message
        .orEmpty()

    @Test
    fun `options given replace the defaults, in any case, the others are kept`() {
        val defaults =
            ServerOptions(tags = listOf("old"), modeConstraint = InstanceModeConstraint.PRIMARY)

        val options =
            parse(
                options(
                    "TAGS" to list("eu", "big"),
                    "modeconstraint" to Values.stringValue("none"),
                ),
                defaults,
            )

        assertThat(options)
            .isEqualTo(
                ServerOptions(
                    tags = listOf("eu", "big"),
                    modeConstraint = InstanceModeConstraint.NONE,
                )
            )
        assertThat(parse(options(), defaults)).isEqualTo(defaults)
    }

    @Test
    fun `without defaults every option must be given`() {
        val message = catchThrowable {
            ServerOptionsInput.parse(
                options("tags" to list("eu")),
                null,
                "Could not enable server 'a'",
                noDefaults = ", and its settings are not known",
            )
        }
            .message

        assertThat(message)
            .isEqualTo(
                "Could not enable server 'a': 'modeConstraint', 'allowedDatabases', " +
                    "'deniedDatabases' not given, and its settings are not known."
            )
    }

    @Test
    fun `unknown options, values of the wrong kind and bad tags are refused`() {
        assertThat(failure(options("tag" to list("eu"))))
            .startsWith("Could not alter server 'a': unrecognised option 'tag'")
        assertThat(failure(options("modeConstraint" to Values.stringValue("BOTH"))))
            .contains("'modeConstraint' is 'BOTH', not one of 'PRIMARY', 'SECONDARY', 'NONE'")
        assertThat(failure(options("tags" to Values.stringValue("eu"))))
            .contains("'tags' is 'eu', not a list of strings")
        assertThat(failure(options("tags" to list("eu", "eu")))).contains("have duplicates")
        assertThat(failure(options("tags" to list("eu,us")))).contains("has a comma")
        assertThat(failure(options("tags" to list("")))).contains("is empty")
    }

    @Test
    fun `allowed and denied databases cannot both be set, but both may be empty`() {
        assertThat(
                failure(options("allowedDatabases" to list("a"), "deniedDatabases" to list("b")))
            )
            .contains("cannot both be set")
        assertThat(parse(options("allowedDatabases" to list(), "deniedDatabases" to list())))
            .isEqualTo(ServerOptions())
    }
}
