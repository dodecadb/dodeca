package dev.dodeca.plugin

import java.io.DataInputStream
import java.nio.file.Path
import java.util.jar.JarFile
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PluginJarIT {
    private val jar = JarFile(Path.of(System.getProperty("dodeca.plugin.jar")).toFile())
    private val entries = jar.entries().toList().map { it.name }

    @AfterAll fun close() = jar.close()

    private fun text(name: String): String =
        jar.getInputStream(requireNotNull(jar.getJarEntry(name)) { "$name is missing" }).use {
            it.readAllBytes().decodeToString()
        }

    private fun services(name: String): List<String> =
        text("META-INF/services/$name")
            .lines()
            .map { it.trim() }
            .filter {
                it.isNotEmpty() && !it.startsWith("#")
            }

    @Test
    fun `Kotlin and Dropwizard Metrics are relocated and nothing from Neo4j's classpath is bundled`() {
        val classes = entries.filter { it.endsWith(".class") }
        val packages = classes.map { it.substringBeforeLast('/') }.toSet()

        assertThat(packages.filter { it.startsWith("kotlin") || it.startsWith("com/codahale") })
            .isEmpty()
        assertThat(packages)
            .contains(
                "dev/dodeca/shaded/kotlin",
                "dev/dodeca/shaded/kotlinx/coroutines",
                "dev/dodeca/shaded/com/codahale/metrics",
                "dev/dodeca/shaded/com/codahale/metrics/jmx",
            )
        assertThat(
                packages.filter {
                    it.startsWith("org/slf4j") ||
                        it.startsWith("scala") ||
                        it.startsWith("org/neo4j") ||
                        it.startsWith("org/dozerdb")
                }
            )
            .isEmpty()
    }

    @Test
    fun `the cloud SDKs and their libraries are relocated, and only Dodeca and Ratis are not`() {
        val classes = entries.filter { it.endsWith(".class") }
        val packages = classes.map { it.substringBeforeLast('/') }.toSet()

        assertThat(packages)
            .contains(
                "dev/dodeca/shaded/software/amazon/awssdk/services/s3",
                "dev/dodeca/shaded/com/azure/storage/blob",
                "dev/dodeca/shaded/com/google/cloud/storage",
                "dev/dodeca/shaded/com/fasterxml/jackson/databind",
                "dev/dodeca/shaded/reactor/core",
                "dev/dodeca/shaded/org/jspecify/annotations",
                "dev/dodeca/shaded/android/annotation",
            )
        // Ratis is not relocated, and kotlinx-coroutines keeps its debugger's stack frames where
        // the debugger looks for them.
        val roots = listOf("dev/dodeca/", "org/apache/ratis/")
        assertThat(classes.filterNot { name -> roots.any { name.startsWith(it) } }).allMatch {
            it.startsWith("_COROUTINE/")
        }
        // Neither a jar index, which hides what it does not list, nor classes of other Java
        // versions, which Java does not load from a jar that is not multi-release.
        assertThat(entries).noneMatch { it == "META-INF/INDEX.LIST" }
        assertThat(entries).noneMatch { it.startsWith("META-INF/versions/") }
    }

    @Test
    fun `every relocated class that a bundled class refers to is in the jar`() {
        val classes = entries.filter { it.endsWith(".class") }.map { it.removeSuffix(".class") }
        val bundled = classes.toSet()
        val missing =
            classes
                .flatMap { name -> referencedClasses(name).map { it to name } }
                .filter { (referenced, _) ->
                    referenced.startsWith("dev/dodeca/shaded/") && referenced !in bundled
                }
                .filterNot { (referenced, _) ->
                    OPTIONAL.any { referenced.startsWith("dev/dodeca/shaded/$it") }
                }

        assertThat(missing.map { (referenced, by) -> "$referenced, referred to by $by" }).isEmpty()
    }

    /** The classes that a class's constant pool names. */
    private fun referencedClasses(name: String): List<String> =
        DataInputStream(jar.getInputStream(jar.getJarEntry("$name.class")).buffered()).use { input
            ->
            input.skipNBytes(8)
            val count = input.readUnsignedShort()
            val texts = arrayOfNulls<String>(count)
            val classes = mutableListOf<Int>()
            var index = 1
            while (index < count) {
                when (val tag = input.readUnsignedByte()) {
                    1 -> texts[index] = input.readUTF()
                    7 -> classes += input.readUnsignedShort()
                    8,
                    16,
                    19,
                    20 -> input.skipNBytes(2)
                    15 -> input.skipNBytes(3)
                    3,
                    4,
                    9,
                    10,
                    11,
                    12,
                    17,
                    18 -> input.skipNBytes(4)
                    5,
                    6 -> {
                        input.skipNBytes(8)
                        index++
                    }
                    else -> error("$name: constant pool tag $tag")
                }
                index++
            }
            classes.map { texts[it]!!.trimStart('[').removePrefix("L").removeSuffix(";") }
        }

    @Test
    fun `registers the Dodeca services`() {
        assertThat(services("org.neo4j.server.startup.EntryPoint"))
            .containsExactly("dev.dodeca.plugin.DodecaEntryPoint")
        assertThat(services("org.neo4j.configuration.SettingsDeclaration"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.core.DodecaSettings",
                "dev.dodeca.core.monitoring.MetricsSettings",
                "dev.dodeca.cluster.internal.ClusterSettings",
                "dev.dodeca.backup.BackupSettings",
                "dev.dodeca.cloud.CloudStorageSettings",
            )
        assertThat(services("org.neo4j.cloud.storage.StorageSystemProviderFactory"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.cloud.S3StorageFactory",
                "dev.dodeca.cloud.GcsStorageFactory",
                "dev.dodeca.cloud.AzureStorageFactory",
            )
        // The SDKs find their HTTP clients as services, under the relocated names.
        assertThat(services("dev.dodeca.shaded.software.amazon.awssdk.http.SdkHttpService"))
            .containsExactly(
                "dev.dodeca.shaded.software.amazon.awssdk.http.apache5.Apache5SdkHttpService"
            )
        assertThat(services("dev.dodeca.shaded.com.azure.core.http.HttpClientProvider"))
            .containsExactly(
                "dev.dodeca.shaded.com.azure.core.http.jdk.httpclient.JdkHttpClientProvider"
            )
        assertThat(services("org.neo4j.kernel.extension.ExtensionFactory"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.backup.source.BackupExtensionFactory",
                "dev.dodeca.core.pagecache.PageCacheWarmupExtensionFactory",
            )
        assertThat(services("org.neo4j.kernel.impl.constraints.ConstraintSemantics"))
            .containsExactly("dev.dodeca.core.constraints.DodecaConstraintSemantics")
        assertThat(services("org.neo4j.cli.CommandProvider"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.backup.cli.BackupCommandProvider",
                "dev.dodeca.backup.cli.RestoreCommandProvider",
                "dev.dodeca.backup.cli.InspectBackupCommandProvider",
                "dev.dodeca.backup.cli.AggregateBackupCommandProvider",
                "dev.dodeca.backup.cli.BackupAggregateCommandProvider",
                "dev.dodeca.cluster.internal.admin.UnbindCommandProvider",
                "dev.dodeca.cluster.internal.admin.UnbindSystemCommandProvider",
                "dev.dodeca.core.importer.DodecaImportCommandProvider",
                "dev.dodeca.core.copy.CopyCommandProvider",
            )
        assertThat(services("org.neo4j.internal.batchimport.IncrementalBatchImporterFactory"))
            .containsExactly("dev.dodeca.core.importer.DodecaIncrementalImporterFactory")
        assertThat(services("org.neo4j.dbms.archive.CheckDatabase"))
            .containsExactly("dev.dodeca.backup.cli.CheckBackup")
        assertThat(services("dev.dodeca.core.store.BackupArtifacts"))
            .containsExactly("dev.dodeca.backup.cli.CommandBackupArtifacts")
        assertThat(services("org.neo4j.wal.checkpoint.CheckPointThresholdPolicy"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.core.checkpoint.ContinuousThresholdPolicy",
                "dev.dodeca.core.checkpoint.VolumetricThresholdPolicy",
            )
        assertThat(services("org.neo4j.configuration.SettingMigrator"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.core.DodecaSettingsMigrator",
                "dev.dodeca.cluster.internal.ClusterSettingsMigrator",
            )
        assertThat(services("org.neo4j.configuration.GroupSetting"))
            .containsExactlyInAnyOrder(
                "dev.dodeca.cluster.internal.DatabasePriorityTag",
                "dev.dodeca.cluster.internal.routing.ServerPolicy",
            )
    }

    @Test
    fun `every registered service implementation is in the jar`() {
        val registrations =
            entries
                .filter { it.startsWith("META-INF/services/") && !it.endsWith("/") }
                .flatMap { file -> services(file.removePrefix("META-INF/services/")) }

        assertThat(registrations).isNotEmpty
        assertThat(registrations.filterNot { "${it.replace('.', '/')}.class" in entries }).isEmpty()
        assertThat(registrations.filter { it.startsWith("kotlin") }).isEmpty()
    }

    @Test
    fun `ships the licenses and notices`() {
        assertThat(text("META-INF/LICENSE"))
            .contains("GNU GENERAL PUBLIC LICENSE", "Version 3, 29 June 2007")
        assertThat(text("META-INF/license/LICENSE.apache-2.0.txt"))
            .contains("Apache License", "Version 2.0, January 2004")
        assertThat(text("META-INF/license/LICENSE.mit.txt"))
            .contains("Azure SDK for Java", "Permission is hereby granted, free of charge")
        assertThat(text("META-INF/license/LICENSE.bsd-3-clause.txt"))
            .contains("Protocol Buffers", "ThreeTen", "RE2/J", "Neither the name of")
        assertThat(text("META-INF/license/LICENSE.bsd-2-clause.txt"))
            .contains("Stax2 API", "Redistribution and use in source and binary forms")
        assertThat(text("META-INF/NOTICE"))
            .contains(
                "Dodeca\nCopyright",
                "Neo4j Sweden AB",
                "DozerDB",
                "Apache Ratis",
                "AWS SDK for Java",
                "Azure SDK for Java",
                "google-cloud-storage",
                "not affiliated with or endorsed by Neo4j, Inc.",
            )
    }

    @Test
    fun `records the versions in the manifest`() {
        val attributes = jar.manifest.mainAttributes

        assertThat(attributes.getValue("Implementation-Title")).isEqualTo("Dodeca")
        assertThat(attributes.getValue("Dodeca-Neo4j-Version"))
            .isEqualTo(System.getProperty("expected.neo4j.version"))
    }

    companion object {
        /**
         * What the bundled classes refer to but never load on the paths Dodeca takes: the AWS SDK's
         * native client, Google App Engine, Google's gRPC transport and its metrics, BlockHound
         * (which Reactor and Ratis's Netty look for), OpenTelemetry's incubating API, and
         * Woodstox's command line validator.
         */
        private val OPTIONAL =
            listOf(
                "software/amazon/awssdk/crt/",
                "com/google/appengine/",
                "io/grpc/alts/",
                "com/google/cloud/opentelemetry/",
                "io/opentelemetry/contrib/gcp/",
                "io/opentelemetry/api/incubator/",
                "reactor/blockhound/",
                "com/ctc/wstx/shaded/msv_core/driver/",
            )
    }
}
