package org.dozendb.cloud.s3

import java.net.URI
import java.nio.file.AccessDeniedException
import java.nio.file.NoSuchFileException
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import software.amazon.awssdk.awscore.exception.AwsErrorDetails
import software.amazon.awssdk.core.exception.SdkClientException
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.services.s3.model.S3Exception

/** What Amazon S3's store makes of S3's answers. */
class S3StoreTest {
    private val store =
        S3Store(
            S3Client.builder()
                .endpointOverride(URI("http://localhost:1"))
                .region(Region.US_EAST_1)
                .build(),
            checksums = false,
        )

    private fun refusal(status: Int, code: String) =
        S3Exception.builder()
            .statusCode(status)
            .awsErrorDetails(
                AwsErrorDetails.builder().errorCode(code).errorMessage("S3 says $code").build()
            )
            .build() as S3Exception

    @Test
    fun `refusals name the URI and say what S3 said`() {
        assertThat(store.failure("films", "a.csv", refusal(403, "AccessDenied")))
            .isInstanceOf(AccessDeniedException::class.java)
            .hasMessage("s3://films/a.csv: S3 says AccessDenied (AccessDenied)")
            .hasCauseInstanceOf(S3Exception::class.java)
        assertThat(store.failure("films", "a.csv", refusal(404, "NoSuchBucket")))
            .isInstanceOf(NoSuchFileException::class.java)
            .hasMessage("s3://films/a.csv: S3 says NoSuchBucket (NoSuchBucket)")
        assertThat(store.failure("films", "a.csv", refusal(412, "PreconditionFailed")))
            .hasMessage(
                "s3://films/a.csv changed while it was read: S3 says PreconditionFailed " +
                    "(PreconditionFailed)"
            )
        assertThat(store.failure("films", "a.csv", refusal(500, "InternalError")))
            .hasMessage("s3://films/a.csv: S3 says InternalError (InternalError)")
        assertThat(store.failure("films", "a.csv", SdkClientException.create("No credentials")))
            .hasMessage("s3://films/a.csv: No credentials")
    }

    @Test
    fun `parts double every thousand up to 1 GiB, making objects of almost 4 TiB`() {
        assertThat(store.partSize(0)).isEqualTo(8 shl 20)
        assertThat(store.partSize(999)).isEqualTo(8 shl 20)
        assertThat(store.partSize(1000)).isEqualTo(16 shl 20)
        assertThat(store.partSize(7000)).isEqualTo(1 shl 30)
        assertThat(store.partSize(9999)).isEqualTo(1 shl 30)
        assertThat(store.partSize(Int.MAX_VALUE)).isEqualTo(1 shl 30)
        assertThat((0 until 10_000).sumOf { store.partSize(it).toLong() })
            .isEqualTo(4_088_000L shl 20)
    }
}
