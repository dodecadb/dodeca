package org.dozendb.plugin

import java.nio.file.Path
import java.util.jar.JarFile
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PluginJarIT {
    private val jar = JarFile(Path.of(System.getProperty("dozendb.plugin.jar")).toFile())
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
                "org/dozendb/shaded/kotlin",
                "org/dozendb/shaded/kotlinx/coroutines",
                "org/dozendb/shaded/com/codahale/metrics",
                "org/dozendb/shaded/com/codahale/metrics/jmx",
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
    fun `registers the DozenDB services`() {
        assertThat(services("org.neo4j.server.startup.EntryPoint"))
            .containsExactly("org.dozendb.plugin.DozenDbEntryPoint")
        assertThat(services("org.neo4j.configuration.SettingsDeclaration"))
            .containsExactlyInAnyOrder(
                "org.dozendb.core.DozenDbSettings",
                "org.dozendb.core.monitoring.MetricsSettings",
                "org.dozendb.cluster.internal.ClusterSettings",
                "org.dozendb.backup.BackupSettings",
            )
        assertThat(services("org.neo4j.kernel.extension.ExtensionFactory"))
            .containsExactly("org.dozendb.backup.source.BackupExtensionFactory")
        assertThat(services("org.neo4j.kernel.impl.constraints.ConstraintSemantics"))
            .containsExactly("org.dozendb.core.constraints.DozenDbConstraintSemantics")
        assertThat(services("org.neo4j.cli.CommandProvider"))
            .containsExactlyInAnyOrder(
                "org.dozendb.backup.cli.BackupCommandProvider",
                "org.dozendb.backup.cli.RestoreCommandProvider",
                "org.dozendb.backup.cli.InspectBackupCommandProvider",
                "org.dozendb.backup.cli.AggregateBackupCommandProvider",
                "org.dozendb.cluster.internal.admin.UnbindCommandProvider",
            )
        assertThat(services("org.neo4j.dbms.archive.CheckDatabase"))
            .containsExactly("org.dozendb.backup.cli.CheckBackup")
        assertThat(services("org.neo4j.configuration.SettingMigrator"))
            .containsExactlyInAnyOrder(
                "org.dozendb.core.DozenDbSettingsMigrator",
                "org.dozendb.cluster.internal.ClusterSettingsMigrator",
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
        assertThat(text("META-INF/NOTICE"))
            .contains("DozenDB\nCopyright", "Neo4j Sweden AB", "DozerDB", "Apache Ratis")
    }

    @Test
    fun `records the versions in the manifest`() {
        val attributes = jar.manifest.mainAttributes

        assertThat(attributes.getValue("Implementation-Title")).isEqualTo("DozenDB")
        assertThat(attributes.getValue("DozenDB-Neo4j-Version"))
            .isEqualTo(System.getProperty("expected.neo4j.version"))
    }
}
