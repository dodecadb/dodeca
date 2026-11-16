package dev.dodeca.core.graphtype

import dev.dodeca.core.StandaloneServer
import dev.dodeca.core.StandaloneServer.Companion.SYSTEM
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.Neo4jException
import org.neo4j.driver.types.Node
import org.neo4j.driver.types.Relationship

/**
 * Graph types as the Cypher Manual's pages on them describe them, run over Bolt: its examples, with
 * what they show and the errors they say each refused one causes.
 */
class GraphTypesTest {
    // Kept when a test fails, for the logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private lateinit var server: StandaloneServer
    private val drivers = mutableListOf<Driver>()

    @BeforeEach
    fun start() {
        server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to true,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
        server.administer("ALTER USER neo4j SET PASSWORD 'admin-secret-1' CHANGE NOT REQUIRED")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private val admin by lazy { driver("neo4j", "admin-secret-1") }

    private fun Driver.run(query: String, database: String = "neo4j"): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(query).list { it.asMap() }
        }

    /** Runs [query], a graph type command, in Cypher 25, which has them. */
    private fun Driver.cypher25(query: String): List<Map<String, Any?>> = run("CYPHER 25 $query")

    private fun Driver.failure(query: String): Neo4jException =
        checkNotNull(catchThrowableOfType(Neo4jException::class.java) { run(query) }) {
            "$query succeeded"
        }

    /** The GQL statuses of [error] and its causes, outermost first. */
    private fun statuses(error: Neo4jException): List<String> =
        generateSequence(error) { it.gqlCause().orElse(null) }.map { it.gqlStatus() }.toList()

    /** The current graph type, a line per element type or constraint, generated names hidden. */
    private fun Driver.graphType(): List<String> =
        specification().lines().map { it.trim().replace(GENERATED, "constraint_x") }

    private fun Driver.specification(): String =
        cypher25("SHOW CURRENT GRAPH TYPE").single()["specification"] as String

    private fun Driver.constraints(): List<String> =
        cypher25(
                "SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, enforcedLabel, classification " +
                    "RETURN type + ' ' + labelsOrTypes[0] + coalesce(' ' + enforcedLabel, '') + " +
                    "' ' + classification AS constraint, name"
            )
            .map {
                val name = it["name"] as String
                (it["constraint"] as String) + if (GENERATED.matches(name)) "" else " $name"
            }
            .sorted()

    @Test
    fun `sets a graph type, shows it in canonical form and as constraints`() {
        admin.cypher25(EXAMPLE)

        assertThat(admin.graphType())
            .containsExactly(
                "{",
                "(:`City` => {`name` :: STRING NOT NULL, `population` :: INTEGER}),",
                "(:`Person` => :`Resident` {`name` :: STRING, `ssn` :: INTEGER}),",
                "(:`Pet` => :`Animal`&`Resident` {`healthCertificate` :: STRING, " +
                    "`insuranceNumber` :: INTEGER, `name` :: STRING}),",
                "(:`Robot` => :`Resident` {`application` :: STRING NOT NULL}),",
                "(:`Resident`)-[:`LIVES_IN` => {`since` :: DATE NOT NULL}]->(:`City` =>),",
                "CONSTRAINT `constraint_x` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`ssn`) " +
                    "IS KEY,",
                "CONSTRAINT `constraint_x` FOR (`n`:`Pet` =>) REQUIRE (`n`.`healthCertificate`) " +
                    "IS UNIQUE,",
                "CONSTRAINT `constraint_x` FOR (`n`:`Pet` =>) REQUIRE (`n`.`insuranceNumber`) IS KEY,",
                "CONSTRAINT `animal_id` FOR (`n`:`Animal`) REQUIRE (`n`.`id`) IS UNIQUE,",
                "CONSTRAINT `company_name` FOR (`n`:`Company`) REQUIRE (`n`.`name`) IS KEY,",
                "CONSTRAINT `resident_address` FOR (`n`:`Resident`) REQUIRE (`n`.`address`) " +
                    "IS :: STRING",
                "}",
            )
        assertThat(admin.constraints())
            .containsExactly(
                "NODE_KEY Company undesignated company_name",
                "NODE_KEY Person undesignated",
                "NODE_KEY Pet undesignated",
                "NODE_LABEL_EXISTENCE Person Resident dependent",
                "NODE_LABEL_EXISTENCE Pet Animal dependent",
                "NODE_LABEL_EXISTENCE Pet Resident dependent",
                "NODE_LABEL_EXISTENCE Robot Resident dependent",
                "NODE_PROPERTY_EXISTENCE City dependent",
                "NODE_PROPERTY_EXISTENCE Robot dependent",
                "NODE_PROPERTY_TYPE City dependent",
                "NODE_PROPERTY_TYPE City dependent",
                "NODE_PROPERTY_TYPE Person dependent",
                "NODE_PROPERTY_TYPE Person dependent",
                "NODE_PROPERTY_TYPE Pet dependent",
                "NODE_PROPERTY_TYPE Pet dependent",
                "NODE_PROPERTY_TYPE Pet dependent",
                "NODE_PROPERTY_TYPE Resident independent resident_address",
                "NODE_PROPERTY_TYPE Robot dependent",
                "NODE_PROPERTY_UNIQUENESS Animal undesignated animal_id",
                "NODE_PROPERTY_UNIQUENESS Pet undesignated",
                "RELATIONSHIP_PROPERTY_EXISTENCE LIVES_IN dependent",
                "RELATIONSHIP_PROPERTY_TYPE LIVES_IN dependent",
                "RELATIONSHIP_SOURCE_LABEL LIVES_IN Resident dependent",
                "RELATIONSHIP_TARGET_LABEL LIVES_IN City dependent",
            )
    }

    @Test
    fun `data must conform to the graph type, which leaves the rest open`() {
        admin.cypher25(EXAMPLE)
        admin.run(
            "CREATE (alice:Person:Resident {name: 'Alice Carlyle', ssn: 987654321}), " +
                "(ozzy:Pet:Resident:Animal {insuranceNumber: 876543210, " +
                "healthCertificate: 'HC789123', name: 'Ozzy'}), " +
                "(nyc:City {name: 'New York City', population: 8097282}), " +
                "(alice)-[:LIVES_IN {since: date('2018-06-15')}]->(nyc), " +
                "(ozzy)-[:LIVES_IN {since: date('2022-08-09')}]->(nyc), " +
                "(:StrayAnimal:Animal:Resident {id: '24.09-172898'}), " +
                "(alice)-[:OWNER_OF {ownershipId: 'GTHD-985'}]->(ozzy)"
        )

        val refused =
            listOf(
                "CREATE (:Person:Resident {name: 'Alice Carlyle', ssn: 987654321})" to "22N79",
                "CREATE (:Person {name: 'Bob', ssn: 123456789})" to "22NB3",
                "CREATE (:Person:Resident {name: 'Bob'})" to "22N77",
                "CREATE (:Person:Resident {name: 'Bob', ssn: 'HNB48182'})" to "22N78",
                "CREATE (:Human)-[:LIVES_IN {since: date('1999-10-17')}]->(:City {name: 'Toronto'})" to
                    "22NB4",
                "MATCH (alice:Person {name: 'Alice Carlyle'}) " +
                    "CREATE (alice)-[:LIVES_IN {since: date('1999-10-17')}]->(:Country {name: 'Canada'})" to
                    "22NB4",
                "MATCH (alice:Person {name: 'Alice Carlyle'}) " +
                    "CREATE (alice)-[:LIVES_IN]->(:City {name: 'Toronto'})" to "22N77",
                "MATCH (alice:Person {name: 'Alice Carlyle'}) " +
                    "CREATE (alice)-[:LIVES_IN {since: '1999-10-17'}]->(:City {name: 'Toronto'})" to
                    "22N78",
            )
        for ((query, status) in refused) {
            val error = admin.failure(query)
            assertThat(error.code())
                .describedAs(query)
                .isEqualTo("Neo.ClientError.Schema.ConstraintValidationFailed")
            assertThat(statuses(error)).describedAs(query).contains(status)
        }
        assertThat(admin.run("MATCH (n) RETURN count(n) AS nodes").single()["nodes"]).isEqualTo(4L)
    }

    @Test
    fun `a graph type that existing data violates is not set, and the old one stays`() {
        admin.cypher25(EXAMPLE)
        val before = admin.specification()
        admin.run(
            "CREATE (ben:Person:Resident {name: 'Benjamin Davis', ssn: 456789123}), " +
                "(zoey:Pet:Resident:Animal {insuranceNumber: 564738291, name: 'Zoey'}), " +
                "(:Robot:Resident {name: 'Gary', application: 'Veterinary medicine'}), " +
                "(ben)-[:OWNER_OF {ownershipId: 'GTHD-985'}]->(zoey)"
        )

        for (element in
            listOf(
                "(:Robot => :Resident {name :: STRING, id :: INTEGER IS KEY})",
                "(:Person)-[r:OWNER_OF => {ownershipId :: INTEGER IS UNIQUE}]->(:Pet)",
            )) {
            val error = admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE SET { $element }")
            assertThat(error.code())
                .describedAs(element)
                .isEqualTo("Neo.DatabaseError.Schema.ConstraintCreationFailed")
        }

        assertThat(admin.specification()).isEqualTo(before)
    }

    @Test
    fun `setting the same graph type again keeps its constraints, and SET takes what SHOW gives`() {
        admin.cypher25(EXAMPLE)
        admin.run("CREATE (:Pet:Resident:Animal {insuranceNumber: 1, name: 'Zoey'})")
        val names = admin.run("SHOW CONSTRAINTS YIELD name").map { it["name"] }
        val specification = admin.specification()

        admin.cypher25(EXAMPLE)
        admin.cypher25("ALTER CURRENT GRAPH TYPE SET $specification")

        assertThat(admin.run("SHOW CONSTRAINTS YIELD name").map { it["name"] })
            .containsExactlyInAnyOrderElementsOf(names)
        assertThat(admin.specification()).isEqualTo(specification)
    }

    @Test
    fun `property types of every kind show in canonical form, which SET takes back`() {
        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE SET { (:A => {x :: INTEGER | STRING, " +
                "y :: LIST<STRING NOT NULL> NOT NULL, z :: ANY NOT NULL, v :: VECTOR<FLOAT32>(3), " +
                "w :: INTEGER NOT NULL | FLOAT NOT NULL}), " +
                "()-[r:R => {id :: INTEGER IS KEY}]->(:A =>), " +
                "CONSTRAINT c FOR (n:C) REQUIRE n.x IS UNIQUE OPTIONS {indexConfig: {}} }"
        )
        val names = admin.run("SHOW CONSTRAINTS YIELD name").map { it["name"] }
        val specification = admin.specification()

        assertThat(admin.graphType())
            .containsExactly(
                "{",
                "(:`A` => {`v` :: VECTOR<FLOAT32 NOT NULL>(3), `w` :: INTEGER NOT NULL | FLOAT NOT NULL, " +
                    "`x` :: STRING | INTEGER, `y` :: LIST<STRING NOT NULL> NOT NULL, `z` :: ANY NOT NULL}),",
                "()-[:`R` => {`id` :: INTEGER}]->(:`A` =>),",
                "CONSTRAINT `c` FOR (`n`:`C`) REQUIRE (`n`.`x`) IS UNIQUE,",
                "CONSTRAINT `constraint_x` FOR ()-[`r`:`R` =>]->() REQUIRE (`r`.`id`) IS KEY",
                "}",
            )
        admin.run("CREATE (:A {y: ['a'], z: 1, w: 2.5})-[:R {id: 1}]->(:A {y: [], z: 'b', w: 3})")
        assertThat(statuses(admin.failure("CREATE (:A {y: ['a'], z: 1, w: 'no'})")))
            .contains("22N78")
        admin.cypher25("ALTER CURRENT GRAPH TYPE SET $specification")
        assertThat(admin.specification()).isEqualTo(specification)
        assertThat(admin.run("SHOW CONSTRAINTS YIELD name").map { it["name"] })
            .containsExactlyInAnyOrderElementsOf(names)
    }

    @Test
    fun `SET replaces everything, constraints made with CREATE CONSTRAINT too`() {
        admin.cypher25(EXAMPLE)
        admin.run("CREATE CONSTRAINT car_constraint FOR (c:Car) REQUIRE c.licensePlate IS UNIQUE")
        assertThat(admin.graphType())
            .contains(
                "CONSTRAINT `car_constraint` FOR (`n`:`Car`) REQUIRE (`n`.`licensePlate`) IS UNIQUE,"
            )

        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE SET { (n:Label => :LabelTwo {property :: STRING}) }"
        )

        assertThat(admin.graphType())
            .containsExactly("{", "(:`Label` => :`LabelTwo` {`property` :: STRING})", "}")
        admin.cypher25("ALTER CURRENT GRAPH TYPE SET { }")
        assertThat(admin.specification()).isEqualTo("{}")
        assertThat(admin.run("SHOW CONSTRAINTS")).isEmpty()
    }

    @Test
    fun `ADD adds element types and constraints, but changes none`() {
        admin.cypher25(EXAMPLE)

        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE ADD { " +
                "(:Company => {name :: STRING, address :: STRING IS UNIQUE}), " +
                "(:Person)-[:WORKS_FOR => {role :: STRING}]->(:Company) }"
        )

        assertThat(admin.graphType())
            .contains(
                "(:`Company` => {`address` :: STRING, `name` :: STRING}),",
                "(:`Person` =>)-[:`WORKS_FOR` => {`role` :: STRING}]->(:`Company` =>),",
                "CONSTRAINT `constraint_x` FOR (`n`:`Company` =>) REQUIRE (`n`.`address`) IS UNIQUE,",
                "CONSTRAINT `company_name` FOR (`n`:`Company` =>) REQUIRE (`n`.`name`) IS KEY,",
            )
        val before = admin.specification()
        val refused =
            listOf(
                "(p:Person => :Resident {name :: STRING, ssn :: INTEGER, nationality :: STRING}) " +
                    "REQUIRE (p.name, p.ssn) IS KEY" to
                    ("22NCA" to
                        "A node element type identified by the label `Person` already exists in " +
                            "the graph type."),
                "()-[:LIVES_IN => {since :: DATE}]->()" to
                    ("22NCB" to
                        "A relationship element type identified by the relationship type " +
                            "`LIVES_IN` already exists in the graph type."),
                "CONSTRAINT pet_address FOR (pet:Pet) REQUIRE pet.address IS NOT NULL" to
                    ("22NC6" to
                        "The independent constraint 'pet_address' is using the same label `Pet` " +
                            "as a node element type."),
                "(:Animal => {type :: STRING NOT NULL})" to
                    ("22NC4" to
                        "The label(s) `Animal` are defined as both identifying and implied."),
                "(:Owner => :Person)" to
                    ("22NC4" to
                        "The label(s) `Person` are defined as both identifying and implied."),
                "(:Ghost =>)-[:HAUNTS =>]->()" to
                    ("22NC5" to
                        "The node element type referenced by '(:`Ghost` =>)' does not exist."),
                // Neo4j's own check of the statement.
                "(:Empty =>)" to
                    ("22NC2" to
                        "must contain one or more implied labels, or at least one property type."),
            )
        for ((element, expected) in refused) {
            val error = admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE ADD { $element }")
            assertThat(statuses(error))
                .describedAs(element)
                .containsExactly("42001", expected.first)
            assertThat(error.gqlCause().get().statusDescription())
                .describedAs(element)
                .endsWith(expected.second)
            assertThat(error.code()).isEqualTo("Neo.ClientError.Statement.SyntaxError")
        }
        assertThat(admin.specification()).isEqualTo(before)
    }

    @Test
    fun `an element type cannot be added where an independent constraint is`() {
        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE SET { " +
                "CONSTRAINT stayed FOR ()-[r:STAYED_IN]->() REQUIRE r.startDate IS NOT NULL }"
        )

        val error =
            admin.failure(
                "CYPHER 25 ALTER CURRENT GRAPH TYPE ADD { ()-[:STAYED_IN => {endDate :: DATE}]->() }"
            )

        assertThat(statuses(error)).containsExactly("42001", "22NC7")
        assertThat(error.gqlCause().get().statusDescription())
            .endsWith(
                "The independent constraint 'stayed' is using the same relationship type " +
                    "`STAYED_IN` as a relationship element type."
            )
    }

    @Test
    fun `ALTER redefines element types but their keys and uniqueness constraints`() {
        admin.cypher25(EXAMPLE)

        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE ALTER { " +
                "(:Robot => :Resident&Machine {application :: STRING NOT NULL, id :: INTEGER NOT NULL}), " +
                "(:Resident)-[:LIVES_IN => {since :: ANY NOT NULL}]->(:City), " +
                "(:Pet => :Resident {name :: STRING}) }"
        )

        assertThat(admin.graphType())
            .contains(
                "(:`Pet` => :`Resident` {`name` :: STRING}),",
                "(:`Robot` => :`Machine`&`Resident` {`application` :: STRING NOT NULL, " +
                    "`id` :: INTEGER NOT NULL}),",
                "(:`Resident`)-[:`LIVES_IN` => {`since` :: ANY NOT NULL}]->(:`City` =>),",
                "CONSTRAINT `constraint_x` FOR (`n`:`Pet` =>) REQUIRE (`n`.`insuranceNumber`) IS KEY,",
            )
        assertThat(admin.constraints())
            .contains(
                "NODE_LABEL_EXISTENCE Robot Machine dependent",
                "RELATIONSHIP_PROPERTY_EXISTENCE LIVES_IN dependent",
            )
            .doesNotContain("RELATIONSHIP_PROPERTY_TYPE LIVES_IN dependent")

        // Neo4j's semantic checks refuse keys and uniqueness, which its plans of ALTER leave out.
        val key =
            admin.failure(
                "CYPHER 25 ALTER CURRENT GRAPH TYPE ALTER { " +
                    "(:City => {name :: STRING NOT NULL, population :: INTEGER IS KEY}) }"
            )
        assertThat(statuses(key)).containsExactly("42001", "22NCF")
        val missing =
            admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE ALTER { (:Car => :Vehicle) }")
        assertThat(statuses(missing)).containsExactly("42001", "22NC5")
        assertThat(missing.gqlCause().get().statusDescription())
            .endsWith("The node element type referenced by '(:`Car` =>)' does not exist.")
    }

    @Test
    fun `DROP drops element types, but their keys, and constraints by name`() {
        admin.cypher25(EXAMPLE)
        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE ADD { (:Company => {name :: STRING}), " +
                "(:Person)-[:WORKS_FOR => {role :: STRING}]->(:Company) }"
        )

        val partial =
            admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE DROP { (:Pet => :Resident&Animal) }")
        assertThat(statuses(partial)).containsExactly("42001", "22NCC")
        assertThat(partial.gqlCause().get().statusDescription())
            .endsWith(
                "The node element type (:`Pet` => :`Animal`&`Resident` {`healthCertificate` :: " +
                    "STRING, `insuranceNumber` :: INTEGER, `name` :: STRING}) identified by the " +
                    "label `Pet` is different to the one specified in the query " +
                    "(:`Pet` => :`Animal`&`Resident`)."
            )
        val inUse = admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE DROP { (:Person => ) }")
        assertThat(statuses(inUse)).containsExactly("42001", "22NCE")
        assertThat(inUse.gqlCause().get().statusDescription())
            .endsWith(
                "The node element type identified by the label `Person` is referenced in the " +
                    "graph type element '()-[:`WORKS_FOR` =>]->()' and cannot be dropped."
            )
        val dependent =
            admin
                .run("SHOW CONSTRAINTS YIELD name, type, labelsOrTypes")
                .single {
                    it["type"] == "NODE_PROPERTY_EXISTENCE" && it["labelsOrTypes"] == listOf("City")
                }["name"]
        val byName =
            admin.failure("CYPHER 25 ALTER CURRENT GRAPH TYPE DROP { CONSTRAINT $dependent }")
        assertThat(statuses(byName)).contains("22N68")
        assertThat(admin.failure("DROP CONSTRAINT $dependent").gqlStatus()).isNotEmpty()

        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE DROP { (:Pet => ), " +
                "(:Resident)-[:LIVES_IN => {since :: DATE NOT NULL}]->(:City), CONSTRAINT animal_id }"
        )
        admin.run("DROP CONSTRAINT resident_address")

        assertThat(admin.graphType())
            .containsExactly(
                "{",
                "(:`City` => {`name` :: STRING NOT NULL, `population` :: INTEGER}),",
                "(:`Company` => {`name` :: STRING}),",
                "(:`Person` => :`Resident` {`name` :: STRING, `ssn` :: INTEGER}),",
                "(:`Robot` => :`Resident` {`application` :: STRING NOT NULL}),",
                "(:`Person` =>)-[:`WORKS_FOR` => {`role` :: STRING}]->(:`Company` =>),",
                "CONSTRAINT `company_name` FOR (`n`:`Company` =>) REQUIRE (`n`.`name`) IS KEY,",
                "CONSTRAINT `constraint_x` FOR (`n`:`Person` =>) REQUIRE (`n`.`name`, `n`.`ssn`) " +
                    "IS KEY,",
                "CONSTRAINT `constraint_x` FOR (`n`:`Pet`) REQUIRE (`n`.`healthCertificate`) IS UNIQUE,",
                "CONSTRAINT `constraint_x` FOR (`n`:`Pet`) REQUIRE (`n`.`insuranceNumber`) IS KEY",
                "}",
            )
        // Pets need not be residents any more.
        admin.run("CREATE (:Pet {insuranceNumber: 1})")
    }

    @Test
    fun `SHOW CURRENT GRAPH TYPE AS GRAPH gives the graph type as a virtual graph`() {
        admin.cypher25(
            "ALTER CURRENT GRAPH TYPE SET { " +
                "(p:Person => :Resident {name :: STRING, ssn :: INTEGER}) REQUIRE (p.name, p.ssn) IS KEY, " +
                "(:City => {name :: STRING NOT NULL}), " +
                "(:Resident)-[:LIVES_IN => {since :: ANY NOT NULL}]->(:City), " +
                "()-[:VISITED => {when :: DATE}]->(:City), " +
                "CONSTRAINT stayed FOR ()-[r:STAYED_IN]->() REQUIRE r.endDate IS :: DATE }"
        )

        val row = admin.cypher25("SHOW CURRENT GRAPH TYPE AS GRAPH").single()
        @Suppress("UNCHECKED_CAST") val nodes = row["nodes"] as List<Node>
        @Suppress("UNCHECKED_CAST") val relationships = row["relationships"] as List<Relationship>

        fun describe(node: Node) = node.labels().single() + node.asMap().toSortedMap()
        assertThat(nodes.map(::describe))
            .containsExactly(
                "AnyLabel{}",
                "AnyLabel{}",
                "NodeElementType{constraints=[], label=City, properties=[name :: STRING NOT NULL]}",
                "NodeElementType{constraints=[CONSTRAINT ${keyName()} FOR (n:Person =>) REQUIRE " +
                    "(n.name, n.ssn) IS KEY], label=Person, properties=[name :: STRING, ssn :: INTEGER]}",
                "NodeLabel{constraints=[], label=Resident}",
            )
        assertThat(nodes.map { it.elementId().toLong() }).allMatch { it < 0 }
        val byId = nodes.associateBy { it.elementId() }
        assertThat(
                relationships.map {
                    val start = byId.getValue(it.startNodeElementId())
                    val end = byId.getValue(it.endNodeElementId())
                    "(${start["label"].asString("")})-[${it.type()} ${it.asMap().toSortedMap()}]->" +
                        "(${end["label"].asString("")})"
                }
            )
            .containsExactly(
                "(Person)-[IMPLIES {}]->(Resident)",
                "(Resident)-[RELATIONSHIP_ELEMENT_TYPE {constraints=[], properties=[since :: ANY " +
                    "NOT NULL], relationshipType=LIVES_IN}]->(City)",
                "()-[RELATIONSHIP_ELEMENT_TYPE {constraints=[], properties=[when :: DATE], " +
                    "relationshipType=VISITED}]->(City)",
                "()-[RELATIONSHIP_TYPE {constraints=[CONSTRAINT stayed FOR ()-[r:STAYED_IN]->() " +
                    "REQUIRE r.endDate IS :: DATE], relationshipType=STAYED_IN}]->()",
            )
        val stayed = relationships.single { it.type() == "RELATIONSHIP_TYPE" }
        assertThat(stayed.startNodeElementId()).isEqualTo(stayed.endNodeElementId())
    }

    private fun keyName(): String =
        admin
            .run("SHOW CONSTRAINTS YIELD name, type WHERE type = 'NODE_KEY' RETURN name")
            .single()["name"]
            as String

    @Test
    fun `SHOW CURRENT GRAPH TYPE combines with other clauses`() {
        admin.cypher25(EXAMPLE)

        val lines =
            admin.cypher25(
                "SHOW CURRENT GRAPH TYPE YIELD specification " +
                    "WHERE specification <> '{}' " +
                    "WITH split(specification, '\\n') AS lines " +
                    "UNWIND lines[1..-1] AS line " +
                    "RETURN rtrim(trim(line), ',') AS element"
            )

        assertThat(lines.map { it["element"] })
            .hasSize(11)
            .contains("(:`Robot` => :`Resident` {`application` :: STRING NOT NULL})")
    }

    /** As the Cypher Manual has it, with NAME MANAGEMENT for labels new to the database. */
    @Test
    fun `showing the graph type takes SHOW CONSTRAINT, setting it CREATE and DROP CONSTRAINT`() {
        admin.cypher25(EXAMPLE)
        admin.run("CREATE ROLE schemer", SYSTEM)
        admin.run("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED", SYSTEM)
        admin.run("GRANT ROLE schemer TO alice", SYSTEM)
        val alice = driver("alice", "alice-secret-1")
        val set = "CYPHER 25 ALTER CURRENT GRAPH TYPE SET { (:Label => :LabelTwo) }"
        val add = "CYPHER 25 ALTER CURRENT GRAPH TYPE ADD { (:Label => :LabelTwo) }"
        val drop = "CYPHER 25 ALTER CURRENT GRAPH TYPE DROP { (:Pet =>) }"

        assertThat(alice.failure("CYPHER 25 SHOW CURRENT GRAPH TYPE").code())
            .isEqualTo("Neo.ClientError.Security.Forbidden")
        admin.run("GRANT SHOW CONSTRAINT ON DATABASE neo4j TO schemer", SYSTEM)
        assertThat(alice.specification()).isEqualTo(admin.specification())

        admin.run("GRANT CREATE CONSTRAINT ON DATABASE neo4j TO schemer", SYSTEM)
        for (query in listOf(set, drop)) {
            assertThat(alice.failure(query).code())
                .describedAs(query)
                .isEqualTo("Neo.ClientError.Security.Forbidden")
        }
        assertThat(alice.failure(add).message).startsWith("Creating new node label")
        admin.run("GRANT CREATE NEW NODE LABEL ON DATABASE neo4j TO schemer", SYSTEM)
        alice.run(add)
        admin.run("GRANT DROP CONSTRAINT ON DATABASE neo4j TO schemer", SYSTEM)
        alice.run(drop)
        alice.run(set)
        assertThat(admin.graphType()).containsExactly("{", "(:`Label` => :`LabelTwo`)", "}")
    }

    @Test
    fun `the graph type survives a restart and is enforced after it`() {
        admin.cypher25(EXAMPLE)
        val before = admin.specification()
        drivers.forEach(Driver::close)
        drivers.clear()

        server.restart()

        val again = driver("neo4j", "admin-secret-1")
        assertThat(again.specification()).isEqualTo(before)
        assertThat(statuses(again.failure("CREATE (:Robot {application: 'x'})"))).contains("22NB3")
    }

    private companion object {
        val GENERATED = Regex("constraint_[0-9a-f]+")

        /** The graph type the Cypher Manual's pages on graph types begin from. */
        const val EXAMPLE =
            "ALTER CURRENT GRAPH TYPE SET { " +
                "(p:Person => :Resident {name :: STRING, ssn :: INTEGER}) " +
                "REQUIRE (p.name, p.ssn) IS KEY, " +
                "(:Pet => :Resident&Animal {insuranceNumber :: INTEGER IS KEY, " +
                "healthCertificate :: STRING IS UNIQUE, name :: STRING}), " +
                "(:Robot => :Resident {application :: STRING NOT NULL}), " +
                "(:City => {name :: STRING NOT NULL, population :: INTEGER}), " +
                "(:Resident)-[:LIVES_IN => {since :: DATE NOT NULL}]->(:City), " +
                "CONSTRAINT company_name FOR (c:Company) REQUIRE c.name IS KEY, " +
                "CONSTRAINT animal_id FOR (a:Animal) REQUIRE a.id IS UNIQUE, " +
                "CONSTRAINT resident_address FOR (resident:Resident) " +
                "REQUIRE resident.address :: STRING }"
    }
}
