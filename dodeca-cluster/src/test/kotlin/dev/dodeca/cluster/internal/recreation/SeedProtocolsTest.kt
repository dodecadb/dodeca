package dev.dodeca.cluster.internal.recreation

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.junit.jupiter.api.Test

/** The seed reports members make when probed, and the seed checksums bulk request. */
class SeedProtocolsTest {
    private val films = UUID.randomUUID()
    private val series = UUID.randomUUID()

    @Test
    fun `seed reports round-trip, with none for a member without a store`() {
        val reports =
            mapOf(
                films to Seed(12, 9, -123456, "1794331998172/-813348991446160266"),
                series to null,
            )

        val text = SeedReports.encode(reports)

        assertThat(SeedReports.decode(text)).isEqualTo(reports)
        assertThat(SeedReports.decode(null)).isEmpty()
        assertThat(SeedReports.encode(emptyMap())).isEmpty()
    }

    @Test
    fun `malformed seed reports are left out`() {
        val good = SeedReports.encode(mapOf(films to Seed(1, 1, 2, "3/4")))

        assertThat(SeedReports.decode("$good,nonsense,$series=1:2,${UUID.randomUUID()}=a:b:c:d"))
            .containsOnlyKeys(films)
    }

    @Test
    fun `seed checksums are asked by append index, known or not`() {
        val request = SeedChecksums.Request("films", films, listOf(3L, 5L))
        val out = ByteArrayOutputStream()

        SeedChecksums.serve(SeedChecksums.encodeRequest(request), out) { asked ->
            assertThat(asked).isEqualTo(request)
            mapOf(3L to 42, 5L to null)
        }

        assertThat(SeedChecksums.decodeResponse(ByteArrayInputStream(out.toByteArray())))
            .isEqualTo(mapOf(3L to 42, 5L to null))
    }

    @Test
    fun `a member that cannot tell seed checksums says why`() {
        val request = SeedChecksums.encodeRequest(SeedChecksums.Request("films", films, listOf(3L)))
        val out = ByteArrayOutputStream()

        SeedChecksums.serve(request, out) { error("this member keeps no seed of database films") }

        assertThat(
                catchThrowable {
                    SeedChecksums.decodeResponse(ByteArrayInputStream(out.toByteArray()))
                }
            )
            .isInstanceOf(IOException::class.java)
            .hasMessage("this member keeps no seed of database films")
    }
}
