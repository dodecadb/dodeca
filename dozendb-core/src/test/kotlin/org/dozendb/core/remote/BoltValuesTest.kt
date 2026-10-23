package org.dozendb.core.remote

import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.OffsetTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import org.neo4j.bolt.connection.netty.impl.async.inbound.ByteBufInput
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonValuePacker
import org.neo4j.bolt.connection.netty.impl.messaging.common.CommonValueUnpacker
import org.neo4j.bolt.connection.netty.impl.packstream.PackOutput
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.CoordinateReferenceSystem
import org.neo4j.values.storable.DateTimeValue
import org.neo4j.values.storable.DateValue
import org.neo4j.values.storable.DurationValue
import org.neo4j.values.storable.LocalDateTimeValue
import org.neo4j.values.storable.LocalTimeValue
import org.neo4j.values.storable.TimeValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues

/**
 * Neo4j's values through bolt-connection's own packing and unpacking, as parameters and results.
 */
class BoltValuesTest {
    /** Packs [value] as a parameter, then unpacks it as a result, as the wire carries it. */
    private fun roundTrip(value: AnyValue): AnyValue {
        val buffer = Unpooled.buffer()
        CommonValuePacker(Output(buffer), true).pack(mapOf("v" to BoltValue(value)))
        val input = ByteBufInput().apply { start(buffer) }
        val unpacked = CommonValueUnpacker(input, true, BoltValues).unpackMap()
        return BoltValues.anyValue(unpacked.getValue("v"))
    }

    @ParameterizedTest
    @MethodSource("values")
    fun `values cross the wire unchanged`(value: AnyValue) {
        assertThat(roundTrip(value)).isEqualTo(value)
    }

    @ParameterizedTest
    @MethodSource("arrays")
    fun `property arrays cross the wire as lists`(value: AnyValue, list: AnyValue) {
        assertThat(roundTrip(value)).isEqualTo(list)
    }

    /** A packstream output that writes into [buffer], without Bolt's chunks. */
    private class Output(private val buffer: ByteBuf) : PackOutput {
        override fun writeByte(value: Byte) = apply { buffer.writeByte(value.toInt()) }

        override fun writeBytes(data: ByteArray) = apply { buffer.writeBytes(data) }

        override fun writeShort(value: Short) = apply { buffer.writeShort(value.toInt()) }

        override fun writeInt(value: Int) = apply { buffer.writeInt(value) }

        override fun writeLong(value: Long) = apply { buffer.writeLong(value) }

        override fun writeDouble(value: Double) = apply { buffer.writeDouble(value) }

        override fun writeFloat(value: Float) = apply { buffer.writeFloat(value) }
    }

    companion object {
        @JvmStatic
        fun values(): List<AnyValue> {
            val stockholm = ZoneId.of("Europe/Stockholm")
            val nested = MapValueBuilder()
            nested.add("list", VirtualValues.list(Values.longValue(1), Values.stringValue("x")))
            nested.add("empty", VirtualValues.EMPTY_MAP)
            return listOf(
                Values.NO_VALUE,
                Values.TRUE,
                Values.FALSE,
                Values.longValue(0),
                Values.longValue(-1),
                Values.longValue(Long.MIN_VALUE),
                Values.longValue(Long.MAX_VALUE),
                Values.intValue(42),
                Values.doubleValue(3.14),
                Values.doubleValue(-0.0),
                Values.doubleValue(Double.MAX_VALUE),
                Values.doubleValue(Double.POSITIVE_INFINITY),
                Values.stringValue(""),
                Values.stringValue("héllo, wörld ✓"),
                Values.stringValue("x".repeat(100_000)),
                Values.byteArray(byteArrayOf(0, 1, -1, 127)),
                Values.byteArray(ByteArray(0)),
                VirtualValues.EMPTY_LIST,
                VirtualValues.list(
                    Values.longValue(1),
                    Values.stringValue("two"),
                    Values.NO_VALUE,
                    VirtualValues.list(Values.TRUE),
                ),
                VirtualValues.EMPTY_MAP,
                nested.build(),
                DateValue.date(LocalDate.of(1, 1, 1)),
                DateValue.date(LocalDate.of(9999, 12, 31)),
                TimeValue.time(OffsetTime.of(23, 59, 59, 999_999_999, ZoneOffset.ofHours(-5))),
                LocalTimeValue.localTime(LocalTime.of(0, 0, 0, 1)),
                LocalDateTimeValue.localDateTime(LocalDateTime.of(2024, 2, 29, 12, 30, 15, 123)),
                DateTimeValue.datetime(
                    ZonedDateTime.of(2024, 3, 31, 1, 59, 59, 0, ZoneOffset.ofHoursMinutes(5, 30))
                ),
                // Around the end of daylight saving time, when a local time occurs twice.
                DateTimeValue.datetime(
                    ZonedDateTime.ofLocal(LocalDateTime.of(2024, 10, 27, 2, 30), stockholm, null)
                        .withLaterOffsetAtOverlap()
                ),
                DateTimeValue.datetime(ZonedDateTime.of(1900, 1, 1, 0, 0, 0, 0, stockholm)),
                DurationValue.duration(14, 3, 3_723, 5),
                DurationValue.duration(-1, -2, -3, -4),
                Values.pointValue(CoordinateReferenceSystem.CARTESIAN, 1.5, -2.0),
                Values.pointValue(CoordinateReferenceSystem.CARTESIAN_3D, 1.0, 2.0, 3.0),
                Values.pointValue(CoordinateReferenceSystem.WGS_84, 18.07, 59.33),
                Values.pointValue(CoordinateReferenceSystem.WGS_84_3D, 18.07, 59.33, 28.0),
            )
        }

        @JvmStatic
        fun arrays(): List<Array<AnyValue>> =
            listOf(
                arrayOf(
                    Values.longArray(longArrayOf(1, 2, 3)),
                    VirtualValues.list(
                        Values.longValue(1),
                        Values.longValue(2),
                        Values.longValue(3),
                    ),
                ),
                arrayOf(
                    Values.stringArray("a", "b"),
                    VirtualValues.list(Values.stringValue("a"), Values.stringValue("b")),
                ),
                arrayOf(Values.charValue('c'), Values.stringValue("c")),
            )
    }
}
