package org.dozendb.cluster.internal.forwarding

import java.time.DateTimeException
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetDateTime
import java.time.OffsetTime
import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import org.neo4j.bolt.connection.BoltProtocolVersion
import org.neo4j.bolt.connection.values.IsoDuration
import org.neo4j.bolt.connection.values.Node
import org.neo4j.bolt.connection.values.Path
import org.neo4j.bolt.connection.values.Point
import org.neo4j.bolt.connection.values.Relationship
import org.neo4j.bolt.connection.values.Segment
import org.neo4j.bolt.connection.values.Type
import org.neo4j.bolt.connection.values.Value
import org.neo4j.bolt.connection.values.ValueFactory
import org.neo4j.bolt.connection.values.Vector
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.ArrayValue
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.ByteArray
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.FloatingPointValue
import org.neo4j.values.storable.IntegralValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.NoValue
import org.neo4j.values.storable.PointValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.ListValue
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.NodeValue
import org.neo4j.values.virtual.PathValue
import org.neo4j.values.virtual.RelationshipValue
import org.neo4j.values.virtual.VirtualValues

/**
 * A value as bolt-connection sends and receives it, which is one of Neo4j's own: parameters go to
 * the wire from Neo4j's values, and values from the wire are made into Neo4j's by [BoltValues],
 * without a driver's values in between.
 */
internal class BoltValue(val value: AnyValue) : Value {
    override fun boltValueType(): Type =
        when (value) {
            is NoValue -> Type.NULL
            is BooleanValue -> Type.BOOLEAN
            is IntegralValue -> Type.INTEGER
            is FloatingPointValue -> Type.FLOAT
            is TextValue -> Type.STRING
            is ByteArray -> Type.BYTES
            is ArrayValue,
            is ListValue -> Type.LIST
            is MapValue -> Type.MAP
            is DateValue -> Type.DATE
            is TimeValue -> Type.TIME
            is LocalTimeValue -> Type.LOCAL_TIME
            is LocalDateTimeValue -> Type.LOCAL_DATE_TIME
            is DateTimeValue -> Type.DATE_TIME
            is DurationValue -> Type.DURATION
            is PointValue -> Type.POINT
            // Nodes, relationships and paths are only received: Bolt takes none as parameters.
            is NodeValue -> Type.NODE
            is RelationshipValue -> Type.RELATIONSHIP
            is PathValue -> Type.PATH
            else -> Type.UNSUPPORTED
        }

    override fun asBoolean(): Boolean = (value as BooleanValue).booleanValue()

    override fun asByteArray(): kotlin.ByteArray = (value as ByteArray).asObjectCopy()

    override fun asString(): String = (value as TextValue).stringValue()

    override fun asLong(): Long = (value as IntegralValue).longValue()

    override fun asDouble(): Double = (value as FloatingPointValue).doubleValue()

    override fun asLocalDate(): LocalDate = (value as DateValue).asObjectCopy()

    override fun asOffsetTime(): OffsetTime = (value as TimeValue).asObjectCopy()

    override fun asLocalTime(): LocalTime = (value as LocalTimeValue).asObjectCopy()

    override fun asLocalDateTime(): LocalDateTime = (value as LocalDateTimeValue).asObjectCopy()

    override fun asZonedDateTime(): ZonedDateTime = (value as DateTimeValue).asObjectCopy()

    override fun asBoltIsoDuration(): IsoDuration {
        val duration = value as DurationValue
        return object : IsoDuration {
            override fun months() = duration.get(ChronoUnit.MONTHS)

            override fun days() = duration.get(ChronoUnit.DAYS)

            override fun seconds() = duration.get(ChronoUnit.SECONDS)

            override fun nanoseconds() = duration.get(ChronoUnit.NANOS).toInt()
        }
    }

    override fun asBoltPoint(): Point {
        val point = value as PointValue
        val coordinates = point.coordinate()
        return object : Point {
            override fun srid() = point.crs.code

            override fun x() = coordinates[0]

            override fun y() = coordinates[1]

            override fun z() = if (coordinates.size > 2) coordinates[2] else Double.NaN
        }
    }

    override fun asBoltMap(): Map<String, Value> {
        val result = LinkedHashMap<String, Value>()
        (value as MapValue).foreach<RuntimeException> { key, entry ->
            result[key] = BoltValue(entry)
        }
        return result
    }

    override fun asBoltVector(): Vector =
        throw UnsupportedOperationException("Neo4j 5 has no vector values")

    override fun isNull(): Boolean = value is NoValue

    override fun isEmpty(): Boolean =
        when (value) {
            is MapValue -> value.isEmpty
            is ListValue -> value.isEmpty
            is ArrayValue -> value.isEmpty
            is TextValue -> value.isEmpty
            else -> false
        }

    override fun keys(): Iterable<String> = (value as? MapValue)?.keySet() ?: emptyList<String>()

    override fun size(): Int =
        when (value) {
            is MapValue -> value.size()
            is ListValue -> value.intSize()
            is ArrayValue -> value.intSize()
            else -> 0
        }

    override fun getBoltValue(key: String): Value = BoltValue((value as MapValue).get(key))

    override fun boltValues(): Iterable<Value> =
        when (value) {
            is ListValue -> value.map { BoltValue(it) }
            is ArrayValue -> (0 until value.intSize()).map { BoltValue(value.value(it)) }
            is MapValue -> asBoltMap().values
            else -> emptyList<Value>()
        }

    override fun containsKey(key: String): Boolean = (value as? MapValue)?.containsKey(key) == true

    override fun equals(other: Any?): Boolean = other is BoltValue && other.value == value

    override fun hashCode(): Int = value.hashCode()

    override fun toString(): String = value.toString()
}

/** Makes Neo4j's values of those bolt-connection reads from the wire. */
internal object BoltValues : ValueFactory {
    override fun value(value: Any?): Value = BoltValue(anyValue(value))

    /** Neo4j's value of [value], which bolt-connection read or built. */
    fun anyValue(value: Any?): AnyValue =
        when (value) {
            null -> Values.NO_VALUE
            is BoltValue -> value.value
            is AnyValue -> value
            is Map<*, *> -> {
                val builder = MapValueBuilder(value.size.coerceAtLeast(1))
                for ((key, entry) in value) builder.add(key as String, anyValue(entry))
                builder.build()
            }
            // Lists, and sets such as the bookmarks of a BEGIN.
            is Collection<*> -> VirtualValues.list(*value.map { anyValue(it) }.toTypedArray())
            is LocalDate -> DateValue.date(value)
            is OffsetTime -> TimeValue.time(value)
            is LocalTime -> LocalTimeValue.localTime(value)
            is LocalDateTime -> LocalDateTimeValue.localDateTime(value)
            is OffsetDateTime -> DateTimeValue.datetime(value)
            is ZonedDateTime -> DateTimeValue.datetime(value)
            is NodeHolder -> value.toValue()
            is RelationshipHolder -> value.toValue()
            is PathHolder -> value.toValue()
            else -> Values.of(value)
        }

    override fun node(
        id: Long,
        elementId: String,
        labels: Collection<String>,
        properties: Map<String, Value>,
    ): Node = NodeHolder(id, elementId, labels, properties)

    override fun relationship(
        id: Long,
        elementId: String,
        start: Long,
        startElementId: String,
        end: Long,
        endElementId: String,
        type: String,
        properties: Map<String, Value>,
    ): Relationship =
        RelationshipHolder(
            id,
            elementId,
            start,
            startElementId,
            end,
            endElementId,
            type,
            properties,
        )

    override fun segment(start: Node, relationship: Relationship, end: Node): Segment = SEGMENT

    override fun path(
        segments: List<Segment>,
        nodes: List<Node>,
        relationships: List<Relationship>,
    ): Path =
        PathHolder(nodes.map { it as NodeHolder }, relationships.map { it as RelationshipHolder })

    override fun isoDuration(months: Long, days: Long, seconds: Long, nanoseconds: Int): Value =
        BoltValue(DurationValue.duration(months, days, seconds, nanoseconds.toLong()))

    override fun point(srid: Int, x: Double, y: Double): Value =
        BoltValue(Values.pointValue(CoordinateReferenceSystem.get(srid), x, y))

    override fun point(srid: Int, x: Double, y: Double, z: Double): Value =
        BoltValue(Values.pointValue(CoordinateReferenceSystem.get(srid), x, y, z))

    override fun vector(elementType: Class<*>, elements: Any): Value =
        throw UnsupportedOperationException("Neo4j 5 has no vector values")

    override fun unsupportedType(
        name: String,
        minProtocolVersion: BoltProtocolVersion,
        extra: Map<String, Value>,
    ): Value = throw UnsupportedOperationException("Bolt type $name is not supported")

    override fun unsupportedDateTimeValue(e: DateTimeException): Value = throw e

    private val SEGMENT = object : Segment {}

    private fun properties(properties: Map<String, Value>): MapValue {
        val builder = MapValueBuilder(properties.size.coerceAtLeast(1))
        for ((key, entry) in properties) builder.add(key, anyValue(entry))
        return builder.build()
    }

    class NodeHolder(
        private val id: Long,
        private val elementId: String,
        private val labels: Collection<String>,
        private val properties: Map<String, Value>,
    ) : Node {
        override fun id() = id

        override fun elementId() = elementId

        fun toValue(): NodeValue =
            VirtualValues.nodeValue(
                id,
                elementId,
                Values.stringArray(*labels.toTypedArray()),
                properties(properties),
            )
    }

    /** A relationship, whose ends a path sets after it is read. */
    class RelationshipHolder(
        private val id: Long,
        private val elementId: String,
        private var start: Long,
        private var startElementId: String,
        private var end: Long,
        private var endElementId: String,
        private val type: String,
        private val properties: Map<String, Value>,
    ) : Relationship {
        override fun setStartAndEnd(
            start: Long,
            startElementId: String,
            end: Long,
            endElementId: String,
        ) {
            this.start = start
            this.startElementId = startElementId
            this.end = end
            this.endElementId = endElementId
        }

        fun toValue(): RelationshipValue =
            VirtualValues.relationshipValue(
                id,
                elementId,
                VirtualValues.node(start, startElementId),
                VirtualValues.node(end, endElementId),
                Values.stringValue(type),
                properties(properties),
            )
    }

    class PathHolder(
        private val nodes: List<NodeHolder>,
        private val relationships: List<RelationshipHolder>,
    ) : Path {
        fun toValue(): PathValue =
            VirtualValues.path(
                nodes.map { it.toValue() }.toTypedArray(),
                relationships.map { it.toValue() }.toTypedArray(),
            )
    }
}
