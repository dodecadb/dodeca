package org.dozendb.core.security

import java.util.UUID
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.PropertyRule
import org.neo4j.values.storable.Values

/** Privileges are shown as the examples of Neo4j's docs show them. */
class PrivilegeTextTest {
    private val neo4j = UUID(0, 1)
    private val names = mapOf(neo4j to "neo4j", UUID(0, 2) to "remote-db")

    private fun commands(
        role: String?,
        vararg privileges: Privilege,
        revoke: Boolean = false,
        version: CypherVersion = CypherVersion.Cypher5,
    ) =
        privileges
            .mapNotNull { PrivilegeText.command(it, role, revoke, names::get, version)?.command }
            .distinct()
            .sorted()

    @Test
    fun `shows the built-in roles as the docs list them`() {
        fun of(role: String) = commands(role, *BuiltInRoles.ALL.getValue(role).toTypedArray())

        assertThat(of("PUBLIC"))
            .containsExactly(
                "GRANT ACCESS ON HOME DATABASE TO `PUBLIC`",
                "GRANT EXECUTE FUNCTION * ON DBMS TO `PUBLIC`",
                "GRANT EXECUTE PROCEDURE * ON DBMS TO `PUBLIC`",
                "GRANT LOAD ON ALL DATA TO `PUBLIC`",
            )
        assertThat(of("reader"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE * TO `reader`",
                "GRANT MATCH {*} ON GRAPH * NODE * TO `reader`",
                "GRANT MATCH {*} ON GRAPH * RELATIONSHIP * TO `reader`",
                "GRANT SHOW CONSTRAINT ON DATABASE * TO `reader`",
                "GRANT SHOW INDEX ON DATABASE * TO `reader`",
            )
        assertThat(of("editor"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE * TO `editor`",
                "GRANT MATCH {*} ON GRAPH * NODE * TO `editor`",
                "GRANT MATCH {*} ON GRAPH * RELATIONSHIP * TO `editor`",
                "GRANT SHOW CONSTRAINT ON DATABASE * TO `editor`",
                "GRANT SHOW INDEX ON DATABASE * TO `editor`",
                "GRANT WRITE ON GRAPH * TO `editor`",
            )
        assertThat(of("publisher"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE * TO `publisher`",
                "GRANT MATCH {*} ON GRAPH * NODE * TO `publisher`",
                "GRANT MATCH {*} ON GRAPH * RELATIONSHIP * TO `publisher`",
                "GRANT NAME MANAGEMENT ON DATABASE * TO `publisher`",
                "GRANT SHOW CONSTRAINT ON DATABASE * TO `publisher`",
                "GRANT SHOW INDEX ON DATABASE * TO `publisher`",
                "GRANT WRITE ON GRAPH * TO `publisher`",
            )
        assertThat(of("architect"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE * TO `architect`",
                "GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO `architect`",
                "GRANT INDEX MANAGEMENT ON DATABASE * TO `architect`",
                "GRANT MATCH {*} ON GRAPH * NODE * TO `architect`",
                "GRANT MATCH {*} ON GRAPH * RELATIONSHIP * TO `architect`",
                "GRANT NAME MANAGEMENT ON DATABASE * TO `architect`",
                "GRANT SHOW CONSTRAINT ON DATABASE * TO `architect`",
                "GRANT SHOW INDEX ON DATABASE * TO `architect`",
                "GRANT WRITE ON GRAPH * TO `architect`",
            )
        assertThat(of("admin"))
            .containsExactly(
                "GRANT ACCESS ON DATABASE * TO `admin`",
                "GRANT ALL DBMS PRIVILEGES ON DBMS TO `admin`",
                "GRANT CONSTRAINT MANAGEMENT ON DATABASE * TO `admin`",
                "GRANT INDEX MANAGEMENT ON DATABASE * TO `admin`",
                "GRANT LOAD ON ALL DATA TO `admin`",
                "GRANT MATCH {*} ON GRAPH * NODE * TO `admin`",
                "GRANT MATCH {*} ON GRAPH * RELATIONSHIP * TO `admin`",
                "GRANT NAME MANAGEMENT ON DATABASE * TO `admin`",
                "GRANT SHOW CONSTRAINT ON DATABASE * TO `admin`",
                "GRANT SHOW INDEX ON DATABASE * TO `admin`",
                "GRANT START ON DATABASE * TO `admin`",
                "GRANT STOP ON DATABASE * TO `admin`",
                "GRANT TRANSACTION MANAGEMENT (*) ON DATABASE * TO `admin`",
                "GRANT WRITE ON GRAPH * TO `admin`",
            )
    }

    @Test
    fun `shows the rows of the built-in roles as the docs list them`() {
        assertThat(
                BuiltInRoles.ALL.getValue("admin")
                    .mapNotNull { PrivilegeText.row(it, names::get) }
                    .map {
                        listOf(it.access, it.action, it.resource, it.graph, it.segment)
                            .joinToString("|")
                    }
            )
            .contains(
                "GRANTED|match|all_properties|*|NODE(*)",
                "GRANTED|write|graph|*|NODE(*)",
                "GRANTED|write|graph|*|RELATIONSHIP(*)",
                "GRANTED|transaction_management|database|*|USER(*)",
                "GRANTED|access|database|*|database",
                "GRANTED|dbms_actions|database|*|database",
                "GRANTED|token|database|*|database",
                "GRANTED|start_database|database|*|database",
            )
        assertThat(
                BuiltInRoles.ALL.getValue("PUBLIC")
                    .mapNotNull { PrivilegeText.row(it, names::get) }
                    .map {
                        "${it.action}|${it.graph}|${it.segment}"
                    }
            )
            .containsExactlyInAnyOrder(
                "access|HOME|database",
                "execute|*|FUNCTION(*)",
                "execute|*|PROCEDURE(*)",
                "load|*|ALL DATA",
            )
    }

    @Test
    fun `shows the examples of the docs`() {
        val access =
            Privilege(Access.GRANTED, PrivilegeAction.ACCESS, PrivilegeScope.Database(neo4j))
        val denied =
            Privilege(Access.DENIED, PrivilegeAction.ACCESS, PrivilegeScope.Database(UUID(0, 2)))

        assertThat(commands("regularUsers", access, denied))
            .containsExactly(
                "DENY ACCESS ON DATABASE `remote-db` TO `regularUsers`",
                "GRANT ACCESS ON DATABASE neo4j TO `regularUsers`",
            )
        assertThat(commands("regularUsers", access, denied, revoke = true))
            .containsExactly(
                "REVOKE DENY ACCESS ON DATABASE `remote-db` FROM `regularUsers`",
                "REVOKE GRANT ACCESS ON DATABASE neo4j FROM `regularUsers`",
            )
        assertThat(commands(null, access, *BuiltInRoles.ALL.getValue("PUBLIC").toTypedArray()))
            .contains(
                "GRANT ACCESS ON DATABASE neo4j TO \$role",
                "GRANT EXECUTE PROCEDURE * ON DBMS TO \$role",
            )
        assertThat(
                commands(
                    "roleLoadCidr",
                    Privilege(
                        Access.DENIED,
                        PrivilegeAction.LOAD_CIDR,
                        PrivilegeScope.Dbms,
                        segment = PrivilegeSegment.Cidr("127.0.0.1/32"),
                    ),
                )
            )
            .containsExactly("DENY LOAD ON CIDR \"127.0.0.1/32\" TO `roleLoadCidr`")
        assertThat(
                commands(
                    "databaseAdminUsers",
                    Privilege(
                        Access.GRANTED,
                        PrivilegeAction.DATABASE_ACTIONS,
                        PrivilegeScope.Database(neo4j),
                    ),
                )
            )
            .containsExactly(
                "GRANT ALL DATABASE PRIVILEGES ON DATABASE neo4j TO `databaseAdminUsers`"
            )
    }

    @Test
    fun `shows qualifiers, immutability and names that need backticks`() {
        fun dbms(action: PrivilegeAction, segment: PrivilegeSegment) =
            Privilege(Access.GRANTED, action, PrivilegeScope.Dbms, segment = segment)

        assertThat(
                commands(
                    "odd `role`",
                    dbms(
                        PrivilegeAction.EXECUTE_BOOSTED,
                        PrivilegeSegment.Procedures("apoc.load.*"),
                    ),
                    dbms(
                        PrivilegeAction.SHOW_SETTING,
                        PrivilegeSegment.Settings("dbms.security.*"),
                    ),
                    dbms(PrivilegeAction.IMPERSONATE, PrivilegeSegment.Users("bob-1")),
                    dbms(PrivilegeAction.EXECUTE_ADMIN, PrivilegeSegment.Database),
                    Privilege(
                        Access.DENIED,
                        PrivilegeAction.TRAVERSE,
                        PrivilegeScope.Home,
                        PrivilegeResource.Graph,
                        PrivilegeSegment.Relationships("KNOWS"),
                        immutable = true,
                    ),
                    Privilege(
                        Access.GRANTED,
                        PrivilegeAction.READ,
                        PrivilegeScope.Database(neo4j),
                        PrivilegeResource.Property("first name"),
                        PrivilegeSegment.Nodes("Person"),
                    ),
                    Privilege(
                        Access.GRANTED,
                        PrivilegeAction.SET_LABEL,
                        PrivilegeScope.AllDatabases,
                        PrivilegeResource.Label("Draft"),
                        PrivilegeSegment.Nodes(null),
                    ),
                    Privilege(
                        Access.GRANTED,
                        PrivilegeAction.MATCH,
                        PrivilegeScope.AllDatabases,
                        PrivilegeResource.AllProperties,
                        PrivilegeSegment.NodePattern(
                            PatternSegment.NodeValuePatternSegment(
                                setOf("Person", "Admin"),
                                "age",
                                Values.longValue(18),
                                PropertyRule.ComparisonOperator.GREATER_THAN_OR_EQUAL,
                            )
                        ),
                    ),
                )
            )
            .containsExactly(
                "DENY IMMUTABLE TRAVERSE ON HOME GRAPH RELATIONSHIP KNOWS TO `odd ``role```",
                "GRANT EXECUTE ADMIN PROCEDURES ON DBMS TO `odd ``role```",
                "GRANT EXECUTE BOOSTED PROCEDURE apoc.load.* ON DBMS TO `odd ``role```",
                "GRANT IMPERSONATE (`bob-1`) ON DBMS TO `odd ``role```",
                "GRANT MATCH {*} ON GRAPH * FOR (n:Admin|Person) WHERE n.age >= 18 TO `odd ``role```",
                "GRANT READ {`first name`} ON GRAPH neo4j NODE Person TO `odd ``role```",
                "GRANT SET LABEL Draft ON GRAPH * TO `odd ``role```",
                "GRANT SHOW SETTING dbms.security.* ON DBMS TO `odd ``role```",
            )
    }

    @Test
    fun `names the database privileges of Cypher 5's DBMS as each Cypher version grants them`() {
        val everywhere =
            listOf(
                    PrivilegeAction.ALTER_DATABASE,
                    PrivilegeAction.SET_DATABASE_ACCESS,
                    PrivilegeAction.SET_DEFAULT_LANGUAGE,
                )
                .map { Privilege(Access.GRANTED, it, PrivilegeScope.AllDatabases) }
                .toTypedArray()

        assertThat(commands("r", *everywhere))
            .containsExactly(
                "GRANT ALTER DATABASE ON DBMS TO `r`",
                "GRANT SET DATABASE ACCESS ON DBMS TO `r`",
                "GRANT SET DATABASE DEFAULT LANGUAGE ON DBMS TO `r`",
            )
        assertThat(commands("r", *everywhere, version = CypherVersion.Cypher25))
            .containsExactly(
                "GRANT ALTER DATABASE ON DATABASE * TO `r`",
                "GRANT SET DATABASE ACCESS ON DATABASE * TO `r`",
                "GRANT SET DATABASE DEFAULT LANGUAGE ON DATABASE * TO `r`",
            )
        // On a database or the home database, only Cypher 25 grants them.
        val one =
            Privilege(Access.DENIED, PrivilegeAction.SET_DEFAULT_LANGUAGE, PrivilegeScope.Home)
        assertThat(commands("r", one, version = CypherVersion.Cypher25))
            .containsExactly("DENY SET DATABASE DEFAULT LANGUAGE ON HOME DATABASE TO `r`")
        assertThat(PrivilegeText.cypher25Only(one)).isTrue()
        assertThat(everywhere.map(PrivilegeText::cypher25Only)).containsOnly(false)
    }

    @Test
    fun `names relationship rules, and rules about list properties as Cypher 25 has them`() {
        fun rule(segment: PrivilegeSegment) =
            Privilege(
                Access.DENIED,
                PrivilegeAction.TRAVERSE,
                PrivilegeScope.AllDatabases,
                PrivilegeResource.Graph,
                segment,
            )
        val relationships =
            rule(
                PrivilegeSegment.RelationshipPattern(
                    PatternSegment.RelValuePatternSegment(
                        setOf("KNOWS", "ACTED IN"),
                        "since",
                        Values.longValue(2000),
                        PropertyRule.ComparisonOperator.LESS_THAN,
                    )
                )
            )
        val lists =
            rule(
                PrivilegeSegment.NodePattern(
                    PatternSegment.NodeValuePatternSegment(
                        emptySet(),
                        "regions",
                        Values.stringValue("EU"),
                        PropertyRule.ComparisonOperator.NOT_VALUE_IN_LIST_PROPERTY,
                    )
                )
            )

        assertThat(commands("r", relationships, lists))
            .containsExactly(
                "DENY TRAVERSE ON GRAPH * FOR ()-[r:`ACTED IN`|KNOWS]-() WHERE r.since < 2000 " +
                    "TO `r`",
                "DENY TRAVERSE ON GRAPH * FOR (n) WHERE NOT 'EU' IN n.regions TO `r`",
            )
        assertThat(PrivilegeText.cypher25Only(relationships)).isFalse()
        assertThat(PrivilegeText.cypher25Only(lists)).isTrue()
    }

    @Test
    fun `has a command for every privilege`() {
        val generator = PrivilegeGenerator(kotlin.random.Random(20261023), listOf(neo4j))
        repeat(2000) {
            val privilege = generator.next()
            assertThat(
                    PrivilegeText.command(privilege, "r", false, names::get, CypherVersion.Cypher25)
                )
                .describedAs("$privilege")
                .isNotNull()
            assertThat(PrivilegeText.row(privilege, names::get))
                .describedAs("$privilege")
                .isNotNull()
        }
    }
}
