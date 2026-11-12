package org.dozendb.core.security

import java.net.InetAddress
import java.util.UUID
import kotlin.random.Random
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.neo4j.internal.kernel.api.security.PatternSegment
import org.neo4j.internal.kernel.api.security.PermissionState.EXPLICIT_DENY
import org.neo4j.internal.kernel.api.security.PermissionState.EXPLICIT_GRANT
import org.neo4j.internal.kernel.api.security.PermissionState.NOT_GRANTED
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.PropertyRule
import org.neo4j.values.storable.Value
import org.neo4j.values.storable.Values

/** The rules of Neo4j's privileges, one by one, then checked at random. */
class PrivilegePolicyTest {
    private val films = UUID(0, 1)
    private val books = UUID(0, 2)

    private fun grant(
        action: PrivilegeAction,
        scope: PrivilegeScope = PrivilegeScope.AllDatabases,
        resource: PrivilegeResource = PrivilegeResource.Database,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = Privilege(Access.GRANTED, action, scope, resource, segment)

    private fun deny(
        action: PrivilegeAction,
        scope: PrivilegeScope = PrivilegeScope.AllDatabases,
        resource: PrivilegeResource = PrivilegeResource.Database,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = Privilege(Access.DENIED, action, scope, resource, segment)

    private fun dbms(
        action: PrivilegeAction,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = grant(action, PrivilegeScope.Dbms, segment = segment)

    private fun denyDbms(
        action: PrivilegeAction,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = deny(action, PrivilegeScope.Dbms, segment = segment)

    private fun policy(vararg privileges: Privilege, home: UUID? = films) =
        PrivilegePolicy(privileges.toList(), home)

    @Test
    fun `access needs a grant and no deny, across roles`() {
        assertThat(policy().access(films)).isEqualTo(NOT_GRANTED)
        assertThat(policy(grant(PrivilegeAction.ACCESS)).access(films)).isEqualTo(EXPLICIT_GRANT)
        assertThat(
                policy(
                        grant(PrivilegeAction.ACCESS),
                        deny(PrivilegeAction.ACCESS, PrivilegeScope.Database(films)),
                    )
                    .access(films)
            )
            .isEqualTo(EXPLICIT_DENY)
    }

    @Test
    fun `named scopes apply to their database only, star to all`() {
        val policy = policy(grant(PrivilegeAction.ACCESS, PrivilegeScope.Database(books)))

        assertThat(policy.access(books)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.access(films)).isEqualTo(NOT_GRANTED)
        assertThat(policy.admin(PrivilegeAction.ACCESS, null)).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `home applies to the home database, or to none`() {
        val home = grant(PrivilegeAction.ACCESS, PrivilegeScope.Home)

        assertThat(policy(home, home = films).access(films)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy(home, home = films).access(books)).isEqualTo(NOT_GRANTED)
        assertThat(policy(home, home = null).access(films)).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `ALL DATABASE PRIVILEGES covers access, not start or transactions`() {
        val policy = policy(grant(PrivilegeAction.DATABASE_ACTIONS))

        assertThat(policy.access(films)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.admin(PrivilegeAction.CREATE_INDEX, films)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.admin(PrivilegeAction.CREATE_LABEL, films)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.admin(PrivilegeAction.START_DATABASE, films)).isEqualTo(NOT_GRANTED)
        assertThat(
                policy.admin(PrivilegeAction.SHOW_TRANSACTION, films, PrivilegeSegment.Users("bob"))
            )
            .isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `ALL DBMS PRIVILEGES covers management and impersonation`() {
        val policy = policy(dbms(PrivilegeAction.DBMS_ACTIONS))

        for (action in
            listOf(
                PrivilegeAction.CREATE_ROLE,
                PrivilegeAction.SHOW_USER,
                PrivilegeAction.SET_PASSWORDS,
                PrivilegeAction.CREATE_DATABASE,
                PrivilegeAction.SET_DATABASE_ACCESS,
                PrivilegeAction.ASSIGN_PRIVILEGE,
                PrivilegeAction.SHOW_ALIAS,
                PrivilegeAction.SHOW_SERVER,
            )) {
            assertThat(policy.admin(action, null)).describedAs("$action").isEqualTo(EXPLICIT_GRANT)
        }
        assertThat(policy.admin(PrivilegeAction.IMPERSONATE, null, PrivilegeSegment.Users("bob")))
            .isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.admin(PrivilegeAction.START_DATABASE, films)).isEqualTo(NOT_GRANTED)
        assertThat(policy.access(films)).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `a deny of part of what is asked denies it`() {
        val policy =
            policy(dbms(PrivilegeAction.USER_MANAGEMENT), denyDbms(PrivilegeAction.SET_PASSWORDS))

        assertThat(policy.admin(PrivilegeAction.ALTER_USER, null)).isEqualTo(EXPLICIT_DENY)
        assertThat(policy.admin(PrivilegeAction.SET_USER_STATUS, null)).isEqualTo(EXPLICIT_GRANT)
    }

    @Test
    fun `transactions are managed per user`() {
        val policy =
            policy(
                grant(
                    PrivilegeAction.TRANSACTION_MANAGEMENT,
                    segment = PrivilegeSegment.Users(null),
                ),
                deny(
                    PrivilegeAction.TERMINATE_TRANSACTION,
                    PrivilegeScope.Database(films),
                    segment = PrivilegeSegment.Users("bob"),
                ),
            )
        fun terminate(database: UUID, user: String) =
            policy.admin(
                PrivilegeAction.TERMINATE_TRANSACTION,
                database,
                PrivilegeSegment.Users(user),
            )

        assertThat(terminate(films, "alice")).isEqualTo(EXPLICIT_GRANT)
        assertThat(terminate(films, "bob")).isEqualTo(EXPLICIT_DENY)
        assertThat(terminate(books, "bob")).isEqualTo(EXPLICIT_GRANT)
        assertThat(
                policy.admin(PrivilegeAction.SHOW_TRANSACTION, films, PrivilegeSegment.Users("bob"))
            )
            .isEqualTo(EXPLICIT_GRANT)
    }

    @Test
    fun `IMPERSONATE names users, DENY carves them out`() {
        val policy =
            policy(
                dbms(PrivilegeAction.IMPERSONATE, PrivilegeSegment.Users("alice")),
                dbms(PrivilegeAction.IMPERSONATE, PrivilegeSegment.Users("bob")),
                denyDbms(PrivilegeAction.IMPERSONATE, PrivilegeSegment.Users("alice")),
            )
        fun impersonate(user: String) =
            policy.admin(PrivilegeAction.IMPERSONATE, null, PrivilegeSegment.Users(user))

        assertThat(impersonate("alice")).isEqualTo(EXPLICIT_DENY)
        assertThat(impersonate("bob")).isEqualTo(EXPLICIT_GRANT)
        assertThat(impersonate("carol")).isEqualTo(NOT_GRANTED)
    }

    private fun execute(glob: String) =
        dbms(PrivilegeAction.EXECUTE, PrivilegeSegment.Procedures(glob))

    private fun boost(glob: String) =
        dbms(PrivilegeAction.EXECUTE_BOOSTED, PrivilegeSegment.Procedures(glob))

    @Test
    fun `procedures run with EXECUTE, boosted with EXECUTE BOOSTED too`() {
        // The examples of the docs' execute privileges page.
        assertThat(policy(execute("db.schema.*")).procedure("db.schema.visualization", false))
            .isEqualTo(Execution(EXPLICIT_GRANT, NOT_GRANTED))
        assertThat(
                policy(
                        execute("*"),
                        denyDbms(
                            PrivilegeAction.EXECUTE,
                            PrivilegeSegment.Procedures("dbms.cluster*"),
                        ),
                    )
                    .procedure("dbms.cluster.status", false)
                    .execute
            )
            .isEqualTo(EXPLICIT_DENY)
        assertThat(policy(execute("*"), boost("db.labels")).procedure("db.labels", false))
            .isEqualTo(Execution(EXPLICIT_GRANT, EXPLICIT_GRANT, EXPLICIT_GRANT))
        assertThat(
                policy(
                        execute("*"),
                        denyDbms(
                            PrivilegeAction.EXECUTE_BOOSTED,
                            PrivilegeSegment.Procedures("db.labels"),
                        ),
                    )
                    .procedure("db.labels", false)
            )
            .isEqualTo(Execution(EXPLICIT_GRANT, EXPLICIT_DENY, EXPLICIT_DENY))
        assertThat(policy(boost("*")).procedure("db.labels", false).execute).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `globs span dots, match one character with a question mark, and ignore case`() {
        // The globbing examples of the docs' DBMS privileges page.
        val names =
            listOf(
                "mine.public.exampleProcedure",
                "mine.public.exampleProcedure1",
                "mine.private.exampleProcedure",
                "mine.private.exampleProcedure2",
                "your.exampleProcedure",
            )
        fun matching(glob: String) = names.filter {
            policy(execute(glob)).procedure(it, false).execute.allowsAccess()
        }

        assertThat(matching("mine.*.exampleProcedure"))
            .containsExactly("mine.public.exampleProcedure", "mine.private.exampleProcedure")
        assertThat(matching("mine.*.exampleProcedure?"))
            .containsExactly("mine.public.exampleProcedure1", "mine.private.exampleProcedure2")
        assertThat(matching("*.exampleProcedure"))
            .containsExactly(
                "mine.public.exampleProcedure",
                "mine.private.exampleProcedure",
                "your.exampleProcedure",
            )
        assertThat(matching("MINE.PUBLIC.*")).hasSize(2)
    }

    @Test
    fun `admin procedures need EXECUTE ADMIN, or EXECUTE and BOOSTED, and no deny`() {
        val admin = dbms(PrivilegeAction.EXECUTE_ADMIN)
        val allowed = Execution(EXPLICIT_GRANT, EXPLICIT_GRANT, EXPLICIT_GRANT)
        val denied = Execution(EXPLICIT_DENY, EXPLICIT_DENY, EXPLICIT_DENY)

        assertThat(policy(admin).procedure("apoc.trigger.add", true)).isEqualTo(allowed)
        assertThat(policy(execute("*"), boost("apoc.*")).procedure("apoc.trigger.add", true))
            .isEqualTo(allowed)
        // EXECUTE alone passes Neo4j's first check, so that its second names the procedure.
        assertThat(policy(execute("*")).procedure("apoc.trigger.add", true))
            .isEqualTo(Execution(EXPLICIT_GRANT, NOT_GRANTED, NOT_GRANTED))
        assertThat(policy(boost("*")).procedure("apoc.trigger.add", true).execute)
            .isEqualTo(NOT_GRANTED)
        assertThat(
                policy(
                        admin,
                        denyDbms(PrivilegeAction.EXECUTE, PrivilegeSegment.Procedures("apoc.*")),
                    )
                    .procedure("apoc.trigger.add", true)
            )
            .isEqualTo(denied)
        assertThat(
                policy(execute("*"), boost("*"), denyDbms(PrivilegeAction.EXECUTE_ADMIN))
                    .procedure("apoc.trigger.add", true)
            )
            .isEqualTo(Execution(EXPLICIT_GRANT, EXPLICIT_DENY, EXPLICIT_DENY))
    }

    @Test
    fun `callers are admins with other procedures as with admin ones, without boosting`() {
        assertThat(policy(execute("*")).procedure("dbms.listConnections", false))
            .isEqualTo(Execution(EXPLICIT_GRANT, NOT_GRANTED, NOT_GRANTED))
        assertThat(
                policy(execute("*"), dbms(PrivilegeAction.EXECUTE_ADMIN))
                    .procedure("dbms.listConnections", false)
            )
            .isEqualTo(Execution(EXPLICIT_GRANT, NOT_GRANTED, EXPLICIT_GRANT))
        assertThat(
                policy(
                        execute("*"),
                        boost("dbms.*"),
                        denyDbms(PrivilegeAction.EXECUTE_ADMIN),
                    )
                    .procedure("dbms.listConnections", false)
            )
            .isEqualTo(Execution(EXPLICIT_GRANT, EXPLICIT_GRANT, EXPLICIT_DENY))
    }

    @Test
    fun `ALL DBMS PRIVILEGES executes and boosts everything, its deny blocks all`() {
        val all = policy(dbms(PrivilegeAction.DBMS_ACTIONS))
        val granted = Execution(EXPLICIT_GRANT, EXPLICIT_GRANT, EXPLICIT_GRANT)

        assertThat(all.procedure("db.labels", false)).isEqualTo(granted)
        assertThat(all.procedure("apoc.trigger.add", true)).isEqualTo(granted)
        assertThat(all.function("apoc.coll.sum"))
            .isEqualTo(Execution(EXPLICIT_GRANT, EXPLICIT_GRANT))
        assertThat(all.setting("dbms.security.auth_enabled")).isEqualTo(EXPLICIT_GRANT)
        assertThat(
                policy(execute("*"), denyDbms(PrivilegeAction.DBMS_ACTIONS))
                    .procedure("db.labels", false)
                    .execute
            )
            .isEqualTo(EXPLICIT_DENY)
    }

    @Test
    fun `functions have their own privileges, apart from procedures'`() {
        val policy =
            policy(
                dbms(PrivilegeAction.EXECUTE, PrivilegeSegment.Functions("apoc.coll.*")),
                execute("apoc.any.*"),
            )

        assertThat(policy.function("apoc.coll.sum").execute).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.function("apoc.any.properties").execute).isEqualTo(NOT_GRANTED)
        assertThat(policy.procedure("apoc.coll.sum", false).execute).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `settings are shown by glob, denied ones not`() {
        val policy =
            policy(
                dbms(PrivilegeAction.SHOW_SETTING, PrivilegeSegment.Settings("*")),
                denyDbms(PrivilegeAction.SHOW_SETTING, PrivilegeSegment.Settings("dbms.security*")),
            )

        assertThat(policy.setting("server.bolt.enabled")).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy.setting("dbms.security.auth_enabled")).isEqualTo(EXPLICIT_DENY)
        assertThat(policy().setting("server.bolt.enabled")).isEqualTo(NOT_GRANTED)
    }

    @Test
    fun `LOAD from all data or from addresses in a range`() {
        val allData = dbms(PrivilegeAction.LOAD, PrivilegeSegment.AllData)
        val local = dbms(PrivilegeAction.LOAD_CIDR, PrivilegeSegment.Cidr("10.0.0.0/8"))
        val inside = InetAddress.getByName("10.1.2.3")
        val outside = InetAddress.getByName("192.168.1.1")

        assertThat(policy(allData).loadAllData()).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy(local).loadFrom(inside)).isEqualTo(EXPLICIT_GRANT)
        assertThat(policy(local).loadFrom(outside)).isEqualTo(NOT_GRANTED)
        assertThat(policy(local).loadFrom(null)).isEqualTo(NOT_GRANTED)
        assertThat(
                policy(
                        allData,
                        denyDbms(PrivilegeAction.LOAD_CIDR, PrivilegeSegment.Cidr("10.0.0.0/8")),
                    )
                    .loadFrom(inside)
            )
            .isEqualTo(EXPLICIT_DENY)
        assertThat(
                policy(denyDbms(PrivilegeAction.LOAD_CIDR, PrivilegeSegment.Cidr("not a range")))
                    .loadFrom(outside)
            )
            .isEqualTo(EXPLICIT_DENY)
    }

    private val none: (String) -> Value = { Values.NO_VALUE }

    private fun nodes(label: String? = null) = PrivilegeSegment.Nodes(label)

    private fun relationships(type: String? = null) = PrivilegeSegment.Relationships(type)

    private fun graph(vararg privileges: Privilege) = policy(*privileges).graph(films)

    @Test
    fun `a node is found through any of its labels, and hidden by a deny on any`() {
        val traverseA =
            grant(
                PrivilegeAction.TRAVERSE,
                resource = PrivilegeResource.Graph,
                segment = nodes("A"),
            )

        assertThat(graph(traverseA).traverseNode(setOf("A", "B"), none)).isTrue
        assertThat(graph(traverseA).traverseNode(setOf("B"), none)).isFalse
        assertThat(graph(traverseA).traverseNode(emptySet(), none)).isFalse
        assertThat(
                graph(
                        traverseA,
                        deny(
                            PrivilegeAction.TRAVERSE,
                            resource = PrivilegeResource.Graph,
                            segment = nodes("B"),
                        ),
                    )
                    .traverseNode(setOf("A", "B"), none)
            )
            .isFalse
        assertThat(
                graph(
                        grant(
                            PrivilegeAction.MATCH,
                            resource = PrivilegeResource.AllProperties,
                            segment = nodes(),
                        )
                    )
                    .traverseNode(emptySet(), none)
            )
            .isTrue
    }

    @Test
    fun `DENY MATCH of some properties hides them, not the node`() {
        val match =
            grant(
                PrivilegeAction.MATCH,
                resource = PrivilegeResource.AllProperties,
                segment = nodes(),
            )
        val denyName =
            deny(
                PrivilegeAction.MATCH,
                resource = PrivilegeResource.Property("name"),
                segment = nodes(),
            )
        val graph = graph(match, denyName)

        assertThat(graph.traverseNode(setOf("A"), none)).isTrue
        assertThat(graph.readNodeProperty(setOf("A"), "name", none)).isFalse
        assertThat(graph.readNodeProperty(setOf("A"), "born", none)).isTrue
        assertThat(
                graph(
                        match,
                        deny(
                            PrivilegeAction.MATCH,
                            resource = PrivilegeResource.AllProperties,
                            segment = nodes("A"),
                        ),
                    )
                    .traverseNode(setOf("A"), none)
            )
            .isFalse
    }

    @Test
    fun `a relationship type is found, and its properties read, by type`() {
        val graph =
            graph(
                grant(
                    PrivilegeAction.MATCH,
                    resource = PrivilegeResource.AllProperties,
                    segment = relationships("ACTED_IN"),
                ),
                deny(
                    PrivilegeAction.READ,
                    resource = PrivilegeResource.Property("salary"),
                    segment = relationships(),
                ),
            )

        assertThat(graph.traverseRelationship("ACTED_IN")).isTrue
        assertThat(graph.traverseRelationship("DIRECTED")).isFalse
        assertThat(graph.readRelationshipProperty("ACTED_IN", "role")).isTrue
        assertThat(graph.readRelationshipProperty("ACTED_IN", "salary")).isFalse
        assertThat(graph.traverseNode(setOf("Person"), none)).isFalse
    }

    @Test
    fun `WRITE and ALL GRAPH PRIVILEGES write everything, MERGE creates and sets`() {
        val write =
            graph(
                grant(PrivilegeAction.WRITE, resource = PrivilegeResource.Graph, segment = nodes()),
                grant(
                    PrivilegeAction.WRITE,
                    resource = PrivilegeResource.Graph,
                    segment = relationships(),
                ),
            )
        val all = graph(grant(PrivilegeAction.GRAPH_ACTIONS, resource = PrivilegeResource.Graph))
        val merge =
            graph(
                grant(
                    PrivilegeAction.MERGE,
                    resource = PrivilegeResource.Property("name"),
                    segment = nodes("A"),
                )
            )

        for (graph in listOf(write, all)) {
            assertThat(graph.createNode(setOf("A", "B"))).isTrue
            assertThat(graph.deleteNode(emptySet())).isTrue
            assertThat(graph.setLabel("C")).isTrue
            assertThat(graph.removeLabel("C")).isTrue
            assertThat(graph.setNodeProperty(setOf("A"), "name")).isTrue
            assertThat(graph.createRelationship("R")).isTrue
            assertThat(graph.setRelationshipProperty("R", "since")).isTrue
        }
        assertThat(write.traverseNode(setOf("A"), none)).isFalse
        assertThat(all.traverseNode(setOf("A"), none)).isTrue
        assertThat(merge.traverseNode(setOf("A"), none)).isTrue
        assertThat(merge.readNodeProperty(setOf("A"), "name", none)).isTrue
        assertThat(merge.readNodeProperty(setOf("A"), "born", none)).isFalse
        assertThat(merge.createNode(setOf("A"))).isTrue
        assertThat(merge.setNodeProperty(setOf("A"), "name")).isTrue
        assertThat(merge.setNodeProperty(setOf("A"), "born")).isFalse
        assertThat(merge.deleteNode(setOf("A"))).isFalse
    }

    @Test
    fun `nodes are created, deleted and set for each of their labels`() {
        val createA =
            grant(
                PrivilegeAction.CREATE_ELEMENT,
                resource = PrivilegeResource.Graph,
                segment = nodes("A"),
            )
        val createB =
            grant(
                PrivilegeAction.CREATE_ELEMENT,
                resource = PrivilegeResource.Graph,
                segment = nodes("B"),
            )
        val setA =
            grant(
                PrivilegeAction.SET_PROPERTY,
                resource = PrivilegeResource.AllProperties,
                segment = nodes("A"),
            )

        assertThat(graph(createA).createNode(setOf("A"))).isTrue
        assertThat(graph(createA).createNode(setOf("A", "B"))).isFalse
        assertThat(graph(createA, createB).createNode(setOf("A", "B"))).isTrue
        assertThat(graph(createA).createNode(emptySet())).isFalse
        assertThat(graph(setA).setNodeProperty(setOf("A", "B"), "name")).isFalse
        assertThat(
                graph(
                        grant(
                            PrivilegeAction.DELETE_ELEMENT,
                            resource = PrivilegeResource.Graph,
                            segment = nodes(),
                        ),
                        deny(
                            PrivilegeAction.DELETE_ELEMENT,
                            resource = PrivilegeResource.Graph,
                            segment = nodes("B"),
                        ),
                    )
                    .deleteNode(setOf("A", "B"))
            )
            .isFalse
    }

    @Test
    fun `labels are set and removed by name or all`() {
        val graph =
            graph(
                grant(
                    PrivilegeAction.SET_LABEL,
                    resource = PrivilegeResource.AllLabels,
                    segment = nodes(),
                ),
                deny(
                    PrivilegeAction.SET_LABEL,
                    resource = PrivilegeResource.Label("Admin"),
                    segment = nodes(),
                ),
                grant(
                    PrivilegeAction.REMOVE_LABEL,
                    resource = PrivilegeResource.Label("Draft"),
                    segment = nodes(),
                ),
            )

        assertThat(graph.setLabel("Draft")).isTrue
        assertThat(graph.setLabel("Admin")).isFalse
        assertThat(graph.removeLabel("Draft")).isTrue
        assertThat(graph.removeLabel("Admin")).isFalse
    }

    @Test
    fun `graph privileges of another database do not apply`() {
        val graph =
            policy(
                    grant(
                        PrivilegeAction.GRAPH_ACTIONS,
                        PrivilegeScope.Database(books),
                        PrivilegeResource.Graph,
                    )
                )
                .graph(films)

        assertThat(graph.traverseNode(setOf("A"), none)).isFalse
        assertThat(graph.createNode(setOf("A"))).isFalse
    }

    @Test
    fun `property rules grant when the value satisfies them, and deny only then`() {
        val classified = { value: Value ->
            { key: String -> if (key == "level") value else Values.NO_VALUE }
        }
        val graph =
            graph(
                grant(
                    PrivilegeAction.TRAVERSE,
                    resource = PrivilegeResource.Graph,
                    segment =
                        PrivilegeSegment.NodePattern(
                            PatternSegment.NodeValuePatternSegment(
                                setOf("Doc"),
                                "level",
                                Values.intValue(3),
                                PropertyRule.ComparisonOperator.LESS_THAN_OR_EQUAL,
                            )
                        ),
                ),
                deny(
                    PrivilegeAction.TRAVERSE,
                    resource = PrivilegeResource.Graph,
                    segment =
                        PrivilegeSegment.NodePattern(
                            PatternSegment.NodeValuePatternSegment(
                                emptySet(),
                                "level",
                                Values.intValue(2),
                                PropertyRule.ComparisonOperator.EQUAL,
                            )
                        ),
                ),
            )

        assertThat(graph.traverseNode(setOf("Doc"), classified(Values.intValue(1)))).isTrue
        assertThat(graph.traverseNode(setOf("Doc"), classified(Values.intValue(2)))).isFalse
        assertThat(graph.traverseNode(setOf("Doc"), classified(Values.intValue(5)))).isFalse
        assertThat(graph.traverseNode(setOf("Note"), classified(Values.intValue(1)))).isFalse
        // Without the property no rule applies: the GRANT does not find it, the DENY fails open.
        assertThat(graph.traverseNode(setOf("Doc"), none)).isFalse
        assertThat(
                graph(
                        grant(
                            PrivilegeAction.TRAVERSE,
                            resource = PrivilegeResource.Graph,
                            segment = nodes(),
                        ),
                        deny(
                            PrivilegeAction.TRAVERSE,
                            resource = PrivilegeResource.Graph,
                            segment =
                                PrivilegeSegment.NodePattern(
                                    PatternSegment.NodeValuePatternSegment(
                                        emptySet(),
                                        "level",
                                        Values.intValue(2),
                                        PropertyRule.ComparisonOperator.EQUAL,
                                    )
                                ),
                        ),
                    )
                    .traverseNode(setOf("Doc"), none)
            )
            .isTrue
    }

    @Test
    fun `agrees with the reference evaluator on random privileges`() {
        val random = Random(20261023)
        val generator = PrivilegeGenerator(random, listOf(films, books))
        repeat(3000) { round ->
            val privileges = List(random.nextInt(1, 7)) { generator.next() }
            val home = listOf(films, books, null).random(random)
            val subject = PrivilegePolicy(privileges, home)
            val reference = ReferencePolicy(privileges, home)
            val context = "round $round, home $home, $privileges"

            for (action in ReferencePolicy.SINGLE - GRAPH_ACTIONS) {
                for (database in listOf(films, books, null)) {
                    for (segment in SEGMENTS) {
                        assertThat(subject.admin(action, database, segment))
                            .describedAs("$action on $database about $segment, $context")
                            .isEqualTo(reference.admin(action, database, segment))
                    }
                }
            }
            for (name in NAMES) {
                for (admin in listOf(false, true)) {
                    assertThat(subject.procedure(name, admin))
                        .describedAs("procedure $name, admin $admin, $context")
                        .isEqualTo(reference.procedure(name, admin))
                }
                assertThat(subject.function(name))
                    .describedAs("function $name, $context")
                    .isEqualTo(reference.function(name))
                assertThat(subject.setting(name))
                    .describedAs("setting $name, $context")
                    .isEqualTo(reference.setting(name))
            }
            assertThat(subject.loadAllData())
                .describedAs(context)
                .isEqualTo(reference.loadAllData())
            for (address in ADDRESSES) {
                assertThat(subject.loadFrom(address))
                    .describedAs("load from $address, $context")
                    .isEqualTo(reference.loadFrom(address))
            }
            for (database in listOf(films, books)) {
                compareGraphs(subject.graph(database), reference.graph(database), context)
            }
        }
    }

    private fun compareGraphs(
        subject: GraphPolicy,
        reference: ReferencePolicy.Graph,
        context: String,
    ) {
        fun check(question: String, actual: Boolean, expected: Boolean) {
            assertThat(actual).describedAs("$question, $context").isEqualTo(expected)
        }
        for (labels in LABEL_SETS) {
            for (value in VALUES) {
                val properties = { key: String -> if (key == "p") value else Values.NO_VALUE }
                check(
                    "traverse $labels with p = $value",
                    subject.traverseNode(labels, properties),
                    reference.traverseNode(labels, properties),
                )
                for (key in KEYS) {
                    check(
                        "read $key of $labels with p = $value",
                        subject.readNodeProperty(labels, key, properties),
                        reference.readNodeProperty(labels, key, properties),
                    )
                }
            }
            check("create $labels", subject.createNode(labels), reference.createNode(labels))
            check("delete $labels", subject.deleteNode(labels), reference.deleteNode(labels))
            for (key in KEYS) {
                check(
                    "set $key of $labels",
                    subject.setNodeProperty(labels, key),
                    reference.setNodeProperty(labels, key),
                )
            }
        }
        for (label in LABELS) {
            check("set label $label", subject.setLabel(label), reference.setLabel(label))
            check("remove label $label", subject.removeLabel(label), reference.removeLabel(label))
        }
        for (type in TYPES) {
            check(
                "traverse $type",
                subject.traverseRelationship(type),
                reference.traverseRelationship(type),
            )
            check(
                "create $type",
                subject.createRelationship(type),
                reference.createRelationship(type),
            )
            check(
                "delete $type",
                subject.deleteRelationship(type),
                reference.deleteRelationship(type),
            )
            for (key in KEYS) {
                check(
                    "read $key of $type",
                    subject.readRelationshipProperty(type, key),
                    reference.readRelationshipProperty(type, key),
                )
                check(
                    "set $key of $type",
                    subject.setRelationshipProperty(type, key),
                    reference.setRelationshipProperty(type, key),
                )
            }
        }
    }

    private companion object {
        val SEGMENTS =
            listOf(
                PrivilegeSegment.Database,
                PrivilegeSegment.Users("alice"),
                PrivilegeSegment.Users("bob"),
            )
        val NAMES =
            listOf(
                "db.labels",
                "db.schema.visualization",
                "apoc.load.json",
                "dbms.security.auth_enabled",
            )
        val ADDRESSES =
            listOf(
                InetAddress.getByName("10.1.2.3"),
                InetAddress.getByName("10.2.0.1"),
                InetAddress.getByName("::1"),
                null,
            )
        val LABELS = listOf("A", "B", "C")
        val LABEL_SETS = listOf(emptySet(), setOf("A"), setOf("B"), setOf("A", "B"), setOf("C"))
        val TYPES = listOf("R", "S")
        val KEYS = listOf("p", "q")
        val VALUES: List<Value> = listOf(Values.NO_VALUE, Values.intValue(1), Values.intValue(2))
        val GRAPH_ACTIONS =
            setOf(
                PrivilegeAction.TRAVERSE,
                PrivilegeAction.READ,
                PrivilegeAction.CREATE_ELEMENT,
                PrivilegeAction.DELETE_ELEMENT,
                PrivilegeAction.SET_LABEL,
                PrivilegeAction.REMOVE_LABEL,
                PrivilegeAction.SET_PROPERTY,
            )
    }
}
