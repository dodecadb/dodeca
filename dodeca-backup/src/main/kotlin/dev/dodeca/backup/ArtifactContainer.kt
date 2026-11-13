package dev.dodeca.backup

import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.EOFException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.time.Instant
import java.time.ZoneOffset.UTC
import java.util.BitSet
import java.util.zip.GZIPInputStream
import org.neo4j.dbms.archive.ArchiveFormat
import org.neo4j.dbms.archive.Dumper
import org.neo4j.dbms.archive.StandardCompressionFormat
import org.neo4j.dbms.archive.backup.BackupMetadataV1
import org.neo4j.dbms.archive.backup.BackupMetadataV2
import org.neo4j.io.fs.OutputStreamWritableChannel

/**
 * Neo4j's `.backup` container, which Community defines and reads: a magic, then for a compressed
 * artifact a zstd stream (with zstd's checksums), holding the metadata and then the body.
 *
 * The metadata is version 2 of Community's: the description's fields and a map, in which Dodeca
 * keeps the keys of [ArtifactHeader] Community has no field for. Community's writer stores only its
 * own key in the map, so Dodeca writes the metadata itself, in the same layout; it reads it back
 * with Community's reader.
 */
internal object ArtifactContainer {
    /** The version of Dodeca's keys and of a differential backup's body. */
    const val FORMAT = 1

    private const val COMPRESSED_MAGIC = "${ArchiveFormat.BACKUP_PREFIX}ZV2"
    private const val UNCOMPRESSED_MAGIC = "${ArchiveFormat.BACKUP_PREFIX}TV2"
    private const val METADATA_VERSION = BackupMetadataV2.VERSION

    private const val KEY_FORMAT = "dodeca.format"
    private const val KEY_SOURCE = "dodeca.source"
    private const val KEY_PREVIOUS_CHECKSUM = "dodeca.previousChecksum"
    private const val KEY_LAST_CHECKSUM = "dodeca.lastChecksum"
    private const val KEY_LAST_COMMIT_TIME = "dodeca.lastCommitTime"

    /** Neo4j's keys of the metadata script, written plainly, and gzipped by Neo4j 2026. */
    private const val KEY_METADATA_SCRIPT = "metadataScript"
    private const val KEY_COMPRESSED_METADATA_SCRIPT = "compressedMetadataScript"

    /** The container for Neo4j's dumper: compressing a stream first writes [header] to it. */
    fun format(header: ArtifactHeader): Dumper.DumpFormat =
        object : Dumper.DumpFormat {
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
                    throw IOException("not a Dodeca backup: unsupported backup format $magic")
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
            val metadata = BackupMetadataV1.readFromStream(body)
            return Opened(header(metadata, readFields(body), compressed), body)
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
        header.lastCommitTime?.let { put(KEY_LAST_COMMIT_TIME, it.toEpochMilli().toString()) }
        header.metadataScript?.let { put(KEY_METADATA_SCRIPT, it) }
    }

    /**
     * The fields after the metadata Neo4j's version 1 has, as Neo4j's version 2 writes them: their
     * count, then each key and value, with its length; Neo4j 2026 writes the metadata script under
     * [KEY_COMPRESSED_METADATA_SCRIPT], gzipped.
     */
    private fun readFields(stream: InputStream): Map<String, String> {
        val data = DataInputStream(stream)
        val count = data.readInt()
        if (count < 0) throw IOException("invalid backup metadata: $count fields")
        return buildMap {
            repeat(count) {
                val key = String(bytes(data), Charsets.UTF_8)
                val value = bytes(data)
                if (key == KEY_COMPRESSED_METADATA_SCRIPT) {
                    GZIPInputStream(ByteArrayInputStream(value)).use {
                        put(KEY_METADATA_SCRIPT, String(it.readAllBytes(), Charsets.UTF_8))
                    }
                } else {
                    put(key, String(value, Charsets.UTF_8))
                }
            }
        }
    }

    private fun bytes(data: DataInputStream): ByteArray {
        val length = data.readInt()
        if (length < 0) throw IOException("invalid backup metadata: a field of $length bytes")
        val bytes = data.readNBytes(length)
        if (bytes.size != length) throw EOFException("the backup metadata ends in a field")
        return bytes
    }

    private fun header(
        metadata: BackupMetadataV1,
        fields: Map<String, String>,
        compressed: Boolean,
    ): ArtifactHeader {
        val format =
            fields[KEY_FORMAT] ?: throw IOException("not a Dodeca backup: it has no $KEY_FORMAT")
        if (format != FORMAT.toString())
            throw IOException("unsupported Dodeca backup format $format")
        if (metadata.isCompressed != compressed) {
            throw IOException("the metadata and the format disagree on compression")
        }
        return ArtifactHeader(
            databaseName = metadata.databaseName,
            databaseId = metadata.databaseId,
            storeId = metadata.storeId,
            time = metadata.backupTime.toInstant(UTC),
            full = metadata.isFull,
            recovered = metadata.isFull && metadata.isRecovered,
            compressed = compressed,
            lowestAppendIndex = metadata.lowestAppendIndex,
            highestAppendIndex = metadata.highestAppendIndex,
            previousChecksum = fields[KEY_PREVIOUS_CHECKSUM]?.let { checksum(it) },
            lastChecksum = fields[KEY_LAST_CHECKSUM]?.let { checksum(it) },
            lastCommitTime = fields[KEY_LAST_COMMIT_TIME]?.let { commitTime(it) },
            source = fields[KEY_SOURCE] ?: throw IOException("the source of the backup is missing"),
            metadataScript = fields[KEY_METADATA_SCRIPT],
        )
    }

    private fun checksum(text: String): Int =
        text.toIntOrNull() ?: throw IOException("invalid checksum $text")

    private fun commitTime(text: String): Instant =
        text.toLongOrNull()?.let(Instant::ofEpochMilli)
            ?: throw IOException("invalid commit time $text")

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
        flags.set(0, header.recovered)
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
