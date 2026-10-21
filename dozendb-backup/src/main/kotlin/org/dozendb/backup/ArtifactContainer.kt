package org.dozendb.backup

import java.io.DataOutputStream
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.ZoneOffset.UTC
import java.util.BitSet
import org.neo4j.dbms.archive.ArchiveFormat
import org.neo4j.dbms.archive.CompressionFormat
import org.neo4j.dbms.archive.StandardCompressionFormat
import org.neo4j.dbms.archive.backup.BackupMetadataV2
import org.neo4j.io.fs.OutputStreamWritableChannel

/**
 * Neo4j's `.backup` container, which Community defines and reads: a magic, then for a compressed
 * artifact a zstd stream (with zstd's checksums), holding the metadata and then the body.
 *
 * The metadata is version 2 of Community's: the description's fields and a map, in which DozenDB
 * keeps the keys of [ArtifactHeader] Community has no field for. Community's writer stores only its
 * own key in the map, so DozenDB writes the metadata itself, in the same layout; it reads it back
 * with Community's reader.
 */
internal object ArtifactContainer {
    /** The version of DozenDB's keys and of a differential backup's body. */
    const val FORMAT = 1

    private const val COMPRESSED_MAGIC = "${ArchiveFormat.BACKUP_PREFIX}ZV2"
    private const val UNCOMPRESSED_MAGIC = "${ArchiveFormat.BACKUP_PREFIX}TV2"
    private const val METADATA_VERSION = BackupMetadataV2.VERSION

    private const val KEY_FORMAT = "dozendb.format"
    private const val KEY_SOURCE = "dozendb.source"
    private const val KEY_PREVIOUS_CHECKSUM = "dozendb.previousChecksum"
    private const val KEY_LAST_CHECKSUM = "dozendb.lastChecksum"

    /** The container for Neo4j's dumper: compressing a stream first writes [header] to it. */
    fun format(header: ArtifactHeader): CompressionFormat =
        object : CompressionFormat {
            override fun compress(stream: OutputStream): OutputStream = open(stream, header)

            override fun decompress(stream: InputStream): InputStream =
                throw UnsupportedOperationException("reading takes ArtifactContainer.read")
        }

    /** Writes [header] to [stream] and returns the stream the body goes to. */
    fun open(stream: OutputStream, header: ArtifactHeader): OutputStream =
        open(stream, header, fields(header))

    /** As [open], with the map holding [fields]: with none, Community writes the same bytes. */
    fun open(
        stream: OutputStream,
        header: ArtifactHeader,
        fields: Map<String, String>,
    ): OutputStream {
        val magic = if (header.compressed) COMPRESSED_MAGIC else UNCOMPRESSED_MAGIC
        stream.write(magic.toByteArray(Charsets.US_ASCII))
        // Kept unwrapped: Neo4j's dumper recognises a zstd stream by its class.
        val body =
            if (header.compressed) StandardCompressionFormat.ZSTD.compress(stream) else stream
        try {
            writeMetadata(body, header, fields)
        } catch (e: IOException) {
            body.close()
            throw e
        }
        return body
    }

    /** An artifact's header, and the stream its body follows in. */
    class Opened(val header: ArtifactHeader, val body: InputStream)

    /** Reads the header of the artifact in [stream]; its body follows in the stream returned. */
    fun read(stream: InputStream): Opened {
        val magic = String(stream.readNBytes(ArchiveFormat.MAGIC_PREFIX_LENGTH), Charsets.US_ASCII)
        val compressed =
            when {
                magic == COMPRESSED_MAGIC -> true
                magic == UNCOMPRESSED_MAGIC -> false
                magic.startsWith(ArchiveFormat.BACKUP_PREFIX) ->
                    throw IOException("not a DozenDB backup: unsupported backup format $magic")
                magic.startsWith(ArchiveFormat.DUMP_PREFIX) ->
                    throw IOException("a database dump, not a backup")
                else -> throw IOException("not a backup")
            }
        val body = if (compressed) StandardCompressionFormat.ZSTD.decompress(stream) else stream
        try {
            val version = body.read()
            if (version != METADATA_VERSION) {
                throw IOException("unsupported backup metadata version $version")
            }
            return Opened(header(BackupMetadataV2.readFromStream(body), compressed), body)
        } catch (e: IOException) {
            body.close()
            throw e
        } catch (e: RuntimeException) {
            body.close()
            throw IOException("invalid backup metadata: ${e.message}", e)
        }
    }

    private fun fields(header: ArtifactHeader): Map<String, String> = buildMap {
        put(KEY_FORMAT, FORMAT.toString())
        put(KEY_SOURCE, header.source)
        header.previousChecksum?.let { put(KEY_PREVIOUS_CHECKSUM, it.toString()) }
        header.lastChecksum?.let { put(KEY_LAST_CHECKSUM, it.toString()) }
    }

    private fun header(metadata: BackupMetadataV2, compressed: Boolean): ArtifactHeader {
        val fields = metadata.additionalFields
        val format =
            fields[KEY_FORMAT] ?: throw IOException("not a DozenDB backup: it has no $KEY_FORMAT")
        if (format != FORMAT.toString())
            throw IOException("unsupported DozenDB backup format $format")
        if (metadata.isCompressed != compressed) {
            throw IOException("the metadata and the format disagree on compression")
        }
        return ArtifactHeader(
            databaseName = metadata.databaseName,
            databaseId = metadata.databaseId,
            storeId = metadata.storeId,
            time = metadata.backupTime.toInstant(UTC),
            full = metadata.isFull,
            compressed = compressed,
            lowestAppendIndex = metadata.lowestAppendIndex,
            highestAppendIndex = metadata.highestAppendIndex,
            previousChecksum = fields[KEY_PREVIOUS_CHECKSUM]?.let { checksum(it) },
            lastChecksum = fields[KEY_LAST_CHECKSUM]?.let { checksum(it) },
            source = fields[KEY_SOURCE] ?: throw IOException("the source of the backup is missing"),
        )
    }

    private fun checksum(text: String): Int =
        text.toIntOrNull() ?: throw IOException("invalid checksum $text")

    // The layout of Community's BackupMetadataV1 and V2, byte for byte.
    private fun writeMetadata(
        stream: OutputStream,
        header: ArtifactHeader,
        fields: Map<String, String>,
    ) {
        val data = DataOutputStream(stream)
        data.write(METADATA_VERSION)
        header.storeId.serialize(OutputStreamWritableChannel(data))
        val uuid = header.databaseId.uuid()
        data.write(2 * Long.SIZE_BYTES)
        data.writeLong(uuid.mostSignificantBits)
        data.writeLong(uuid.leastSignificantBits)
        val name = header.databaseName.toByteArray(Charsets.UTF_8)
        require(name.size <= 0xFF) { "database name ${header.databaseName} is too long" }
        data.write(name.size)
        data.write(name)
        data.writeLong(header.time.epochSecond)
        data.writeLong(header.lowestAppendIndex)
        data.writeLong(header.highestAppendIndex)
        val flags = BitSet(3)
        flags.set(0, header.full) // recovered: a full backup holds a recovered store
        flags.set(1, header.compressed)
        flags.set(2, header.full)
        val flagBytes = flags.toByteArray()
        data.write(flagBytes.size)
        data.write(flagBytes)
        data.writeInt(fields.size)
        for ((key, value) in fields.toSortedMap()) {
            for (text in listOf(key, value)) {
                val bytes = text.toByteArray(Charsets.UTF_8)
                data.writeInt(bytes.size)
                data.write(bytes)
            }
        }
        data.flush()
    }
}
