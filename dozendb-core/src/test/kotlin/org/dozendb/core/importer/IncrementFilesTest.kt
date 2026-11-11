package org.dozendb.core.importer

import java.nio.file.Path
import java.time.ZoneOffset
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import org.neo4j.internal.batchimport.cache.NumberArrayFactories
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

/** The files of an increment: its entities, their values and tokens, and the build's keys. */
class IncrementFilesTest {
    @TempDir lateinit var root: Path

    /** The values Neo4j's CSV reader gives, for each type the import takes. */
    private val values: List<Any> =
        listOf(
            42,
            42L,
            1.5f,
            2.5,
            true,
            7.toByte(),
            8.toShort(),
            'x',
            "text",
            intArrayOf(1, 2),
            longArrayOf(3, 4),
            arrayOf("a", "b"),
            booleanArrayOf(true, false),
            doubleArrayOf(1.0, 2.0),
            Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.0, 2.0),
            DateValue.date(2026, 11, 10),
            LocalTimeValue.localTime(12, 30, 15, 5),
            TimeValue.time(12, 30, 15, 5, ZoneOffset.ofHours(2)),
            LocalDateTimeValue.localDateTime(2026, 11, 10, 12, 30, 15, 5),
            DateTimeValue.datetime(2026, 11, 10, 12, 30, 15, 5, "Europe/Brussels"),
            DateTimeValue.datetime(2026, 11, 10, 12, 30, 15, 5, ZoneOffset.ofHours(-3)),
            DurationValue.duration(1, 2, 3, 4),
            arrayOf(
                Values.pointValue(CoordinateReferenceSystem.WGS_84, 4.0, 50.0),
                Values.pointValue(CoordinateReferenceSystem.WGS_84, 5.0, 51.0),
            ),
            arrayOf(DateValue.date(2026, 1, 1), DateValue.date(2027, 1, 1)),
            arrayOf(DurationValue.duration(0, 1, 0, 0)),
            arrayOf(
                DateTimeValue.datetime(2026, 11, 10, 12, 0, 0, 0, "UTC"),
                DateTimeValue.datetime(2026, 11, 10, 13, 0, 0, 0, "UTC"),
            ),
        )

    @Test
    fun `entities keep every value type the import reads`() {
        val path = root.resolve("nodes")
        EntityWriter(path).use { writer ->
            writer.write(
                NewNode(
                    intArrayOf(0, 3),
                    values.mapIndexed { i, value -> Property(i, EntityWriter.storable(value)) },
                )
            )
            writer.write(NewNode(IntArray(0), emptyList()))
        }

        EntityReader(path, relationships = false).use { reader ->
            val node = checkNotNull(reader.node())
            assertThat(node.labels).containsExactly(0, 3)
            assertThat(node.properties.map { it.key }).isEqualTo(values.indices.toList())
            node.properties.forEach { property ->
                assertThat(Values.of(property.value))
                    .describedAs("property ${property.key}")
                    .isEqualTo(neo4jValue(values[property.key]))
            }
            assertThat(checkNotNull(reader.node()).properties).isEmpty()
            assertThat(reader.node()).isNull()
        }
    }

    private fun neo4jValue(value: Any): Value =
        when {
            value is Value -> value
            value is Array<*> && value[0] is Value ->
                Values.of(value.map { (it as Value).asObjectCopy() }.let(::typedArray))
            else -> Values.of(value)
        }

    private fun typedArray(objects: List<Any>): Any {
        val array = java.lang.reflect.Array.newInstance(objects[0].javaClass, objects.size)
        objects.forEachIndexed { i, o -> java.lang.reflect.Array.set(array, i, o) }
        return array
    }

    @Test
    fun `relationships keep their ends and can be read from a later one on`() {
        val path = root.resolve("relationships")
        EntityWriter(path).use { writer ->
            writer.write(NewRelationship(0, 17, NewRelationship.new(0), emptyList()))
            writer.write(
                NewRelationship(1, NewRelationship.new(5), 0, listOf(Property(2, "since")))
            )
        }

        EntityReader(path, skip = 1, relationships = true).use { reader ->
            val relationship = checkNotNull(reader.relationship())
            assertThat(relationship.type).isEqualTo(1)
            assertThat(NewRelationship.isNew(relationship.start)).isTrue
            assertThat(NewRelationship.of(relationship.start)).isEqualTo(5)
            assertThat(NewRelationship.isNew(relationship.end)).isFalse
            assertThat(NewRelationship.of(relationship.end)).isEqualTo(0)
            assertThat(relationship.properties.single().value).isEqualTo("since")
            assertThat(reader.relationship()).isNull()
        }
    }

    @Test
    fun `tokens keep their names by index`() {
        val tokens = Tokens()
        assertThat(tokens.labels.of("Person")).isZero
        assertThat(tokens.labels.of("Actor")).isEqualTo(1)
        assertThat(tokens.labels.of("Person")).isZero
        tokens.types.of("KNOWS")
        tokens.keys.of("née")
        tokens.write(root.resolve("tokens"))

        val read = Tokens.read(root.resolve("tokens"))

        assertThat(read.labels.names).containsExactly("Person", "Actor")
        assertThat(read.types.names).containsExactly("KNOWS")
        assertThat(read.keys.names).containsExactly("née")
        assertThat(read.labels.of("Actor")).isEqualTo(1)
    }

    @Test
    fun `the build's keys are looked up by their place`() {
        KeyStore(
                root.resolve("ids"),
                NumberArrayFactories.HEAP,
                EmptyMemoryTracker.INSTANCE,
            )
            .use { keys ->
                keys.add(3, "c")
                keys.add(0, 42L)
                keys.add(1, "ünïcode")
                keys.complete()

                keys.newLookup().use { lookup ->
                    val tracker = EmptyMemoryTracker.INSTANCE
                    assertThat(lookup.lookupProperty(0, tracker)).isEqualTo(42L)
                    assertThat(lookup.lookupProperty(1, tracker)).isEqualTo("ünïcode")
                    assertThat(lookup.lookupProperty(2, tracker)).isNull()
                    assertThat(lookup.lookupProperty(3, tracker)).isEqualTo("c")
                }
            }
    }
}
