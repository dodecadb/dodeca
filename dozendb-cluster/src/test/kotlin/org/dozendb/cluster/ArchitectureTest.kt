package org.dozendb.cluster

import com.tngtech.archunit.base.DescribedPredicate.not
import com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAPackage
import com.tngtech.archunit.core.domain.JavaClass.Predicates.resideInAnyPackage
import com.tngtech.archunit.core.importer.ClassFileImporter
import com.tngtech.archunit.core.importer.ImportOption
import com.tngtech.archunit.lang.syntax.ArchRuleDefinition.noClasses
import org.junit.jupiter.api.Test

class ArchitectureTest {
    private val classes =
        ClassFileImporter()
            .withImportOption(ImportOption.DoNotIncludeTests())
            .importPackages("org.dozendb.cluster")

    @Test
    fun `no Ratis type is used outside consensus-ratis`() {
        noClasses()
            .should()
            .dependOnClassesThat()
            .resideInAPackage("org.apache.ratis..")
            .check(classes)
    }

    @Test
    fun `Neo4j internals are used only under internal`() {
        val neo4jInternals =
            resideInAPackage("org.neo4j..")
                .and(not(resideInAnyPackage("org.neo4j.procedure..", "org.neo4j.logging..")))
        noClasses()
            .that()
            .resideOutsideOfPackage("org.dozendb.cluster.internal..")
            .should()
            .dependOnClassesThat(neo4jInternals)
            .check(classes)
    }
}
