package org.dozendb.core.cypher

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.catchThrowable
import org.assertj.core.api.Assertions.catchThrowableOfType
import org.dozendb.core.StandaloneServer
import org.dozendb.core.StandaloneServer.Companion.SYSTEM
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.Driver
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.SessionConfig
import org.neo4j.driver.exceptions.ClientException

/**
 * CREATE, DROP and SHOW ALIASES for local aliases, and aliases used wherever a database name goes,
 * over Bolt as drivers see them, by the admin `neo4j` unless said otherwise.
 */
class AliasCommandsTest {
    // Kept when a test fails, for the server's logs.
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
        server.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        server.administer("CREATE DATABASE films WAIT")
        server.rows("CREATE (:Film {title: 'Alien'})", database = "films")
    }

    @AfterEach
    fun stop() {
        drivers.forEach(Driver::close)
        server.close()
    }

    private fun driver(user: String, password: String): Driver =
        GraphDatabase.driver("bolt://${server.boltAddress()}", AuthTokens.basic(user, password))
            .also { drivers += it }

    private val admin by lazy { driver("neo4j", "neo4j-secret-1") }

    private fun Driver.run(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
        database: String = SYSTEM,
    ): List<Map<String, Any?>> =
        session(SessionConfig.forDatabase(database)).use { session ->
            session.run(cypher, parameters).list { it.asMap() }
        }

    private fun Driver.refused(
        cypher: String,
        parameters: Map<String, Any> = emptyMap(),
    ): ClientException =
        checkNotNull(
            catchThrowableOfType(ClientException::class.java) { run(cypher, parameters) }
        ) {
            "$cypher succeeded"
        }

    private val titles = "MATCH (f:Film) RETURN f.title AS title"

    private fun aliases(): List<Map<String, Any?>> =
        admin.run("SHOW ALIASES FOR DATABASE YIELD * RETURN *")

    @Test
    fun `creates, shows and drops local aliases`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")

        assertThat(admin.run("SHOW ALIASES FOR DATABASE"))
            .containsExactly(
                mapOf(
                    "name" to "movies",
                    "composite" to null,
                    "database" to "films",
                    "location" to "local",
                    "url" to null,
                    "user" to null,
                )
            )
        assertThat(aliases().single())
            .containsEntry("driver", null)
            .containsEntry("properties", emptyMap<String, Any>())

        admin.run("DROP ALIAS movies FOR DATABASE")

        assertThat(aliases()).isEmpty()
    }

    @Test
    fun `uses an alias wherever a database name goes`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")

        // A session, its database name, and USE from another database.
        assertThat(admin.run("MATCH (f:Film) RETURN f.title AS title", database = "movies"))
            .containsExactly(mapOf("title" to "Alien"))
        assertThat(admin.run("CALL db.info() YIELD name", database = "movies"))
            .containsExactly(mapOf("name" to "films"))
        assertThat(
                admin.run("USE movies MATCH (f:Film) RETURN f.title AS title", database = "neo4j")
            )
            .containsExactly(mapOf("title" to "Alien"))
        admin.run("CREATE (:Film {title: 'Aliens'})", database = "movies")
        assertThat(server.rows("MATCH (f:Film) RETURN count(f) AS films", database = "films"))
            .containsExactly(mapOf("films" to 2L))
        // The embedded API.
        assertThat(server.rows("MATCH (f:Film) RETURN count(f) AS films", database = "movies"))
            .containsExactly(mapOf("films" to 2L))
        // SHOW DATABASES lists the alias with its database, which SHOW DATABASE also finds by it.
        assertThat(admin.run("SHOW DATABASES YIELD name, aliases WHERE name = 'films'"))
            .containsExactly(mapOf("name" to "films", "aliases" to listOf("movies")))
        assertThat(admin.run("SHOW DATABASE movies YIELD name"))
            .containsExactly(mapOf("name" to "films"))
        // Routing tables, which drivers that route ask for by the name they were given.
        val routing =
            admin.run(
                "CALL dbms.routing.getRoutingTable({}, 'movies') YIELD servers " +
                    "UNWIND servers AS server RETURN server.role AS role",
                database = "neo4j",
            )
        assertThat(routing.map { it["role"] }).contains("WRITE", "READ", "ROUTE")
    }

    @Test
    fun `creates aliases that exist or drops aliases that do not only when told not to mind`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")

        admin.run("CREATE ALIAS movies IF NOT EXISTS FOR DATABASE neo4j")
        admin.run("CREATE ALIAS neo4j IF NOT EXISTS FOR DATABASE films")
        assertThat(aliases().map { it["name"] to it["database"] })
            .containsExactly("movies" to "films")

        admin.run("CREATE OR REPLACE ALIAS movies FOR DATABASE neo4j PROPERTIES {Rating: 5}")
        assertThat(aliases().map { Triple(it["name"], it["database"], it["properties"]) })
            .containsExactly(Triple("movies", "neo4j", mapOf("rating" to 5L)))

        admin.run("DROP ALIAS missing IF EXISTS FOR DATABASE")
        admin.run("DROP ALIAS movies IF EXISTS FOR DATABASE")
        assertThat(aliases()).isEmpty()
    }

    @Test
    fun `alters the target and properties of local aliases`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films PROPERTIES {rating: 5}")
        admin.run("CREATE (:Film {title: 'Brazil'})", database = "neo4j")
        val titles = "MATCH (f:Film) RETURN f.title AS title"

        admin.run("ALTER ALIAS movies SET DATABASE TARGET neo4j")

        assertThat(aliases().map { Triple(it["name"], it["database"], it["properties"]) })
            .containsExactly(Triple("movies", "neo4j", mapOf("rating" to 5L)))
        assertThat(admin.run(titles, database = "movies"))
            .containsExactly(mapOf("title" to "Brazil"))
        assertThat(admin.run("SHOW DATABASES YIELD name, aliases WHERE name <> 'system'"))
            .containsExactlyInAnyOrder(
                mapOf("name" to "films", "aliases" to emptyList<String>()),
                mapOf("name" to "neo4j", "aliases" to listOf("movies")),
            )

        // Properties are replaced, not merged.
        admin.run(
            "ALTER ALIAS \$alias SET DATABASE PROPERTIES {Genre: 'science fiction'}",
            mapOf("alias" to "Movies"),
        )
        assertThat(aliases().map { Triple(it["name"], it["database"], it["properties"]) })
            .containsExactly(Triple("movies", "neo4j", mapOf("genre" to "science fiction")))

        admin.run(
            "ALTER ALIAS movies SET DATABASE TARGET \$target PROPERTIES {}",
            mapOf("target" to "films"),
        )
        assertThat(aliases().map { Triple(it["name"], it["database"], it["properties"]) })
            .containsExactly(Triple("movies", "films", emptyMap<String, Any>()))
        assertThat(admin.run(titles, database = "movies"))
            .containsExactly(mapOf("title" to "Alien"))
    }

    @Test
    fun `refuses to alter aliases that do not exist, or to point them at what is not a database`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run("CREATE ALIAS classics FOR DATABASE films")

        for (name in listOf("missing", "films")) {
            assertThat(admin.refused("ALTER ALIAS $name SET DATABASE TARGET neo4j"))
                .hasMessage(
                    "Failed to alter the specified database alias '$name': " +
                        "Database alias does not exist."
                )
        }
        admin.run("ALTER ALIAS missing IF EXISTS SET DATABASE TARGET neo4j")

        val failed = "Failed to alter the specified database alias 'movies'"
        assertThat(admin.refused("ALTER ALIAS movies SET DATABASE TARGET nothing"))
            .hasMessage("$failed: Database 'nothing' does not exist.")
        assertThat(admin.refused("ALTER ALIAS movies SET DATABASE TARGET system"))
            .hasMessage(
                "$failed: Target database name 'system' is invalid, due to the prefix 'system'."
            )
        for (target in listOf("classics", "movies")) {
            assertThat(admin.refused("ALTER ALIAS movies SET DATABASE TARGET $target"))
                .hasMessage(
                    "$failed: '$target' is an alias, and aliases can only target databases."
                )
        }
        assertThat(aliases().map { it["name"] to it["database"] })
            .containsExactlyInAnyOrder("movies" to "films", "classics" to "films")
        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] })
            .doesNotContain("missing")
    }

    @Test
    fun `keeps a running transaction on the database its alias pointed at when it began`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        val titles = "MATCH (f:Film) RETURN f.title AS title"

        admin.session(SessionConfig.forDatabase("movies")).use { session ->
            session.beginTransaction().use { transaction ->
                assertThat(transaction.run(titles).list { it.asMap() })
                    .containsExactly(mapOf("title" to "Alien"))

                admin.run("ALTER ALIAS movies SET DATABASE TARGET neo4j")

                transaction.run("CREATE (:Film {title: 'Aliens'})")
                transaction.commit()
            }
        }

        assertThat(admin.run(titles, database = "films").map { it["title"] })
            .containsExactlyInAnyOrder("Alien", "Aliens")
        assertThat(admin.run(titles, database = "movies")).isEmpty()
    }

    @Test
    fun `refuses to drop a database that aliases target, unless told to drop them too`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run("CREATE ALIAS classics FOR DATABASE films PROPERTIES {rating: 5}")
        admin.run("CREATE ALIAS home FOR DATABASE neo4j")

        for (command in
            listOf(
                "DROP DATABASE films",
                "DROP DATABASE films IF EXISTS",
                "CREATE OR REPLACE DATABASE films",
            )) {
            val error = admin.refused(command)
            assertThat(error)
                .describedAs(command)
                .hasMessage(
                    "Failed to delete the specified database 'films': Database 'films' has " +
                        "aliases 'classics', 'movies'. Drop them before the database, or drop " +
                        "the database with CASCADE ALIASES."
                )
            assertThat(error.code()).isEqualTo("Neo.ClientError.Database.ExistingAliasFound")
            assertThat(error.gqlStatus()).isEqualTo("42N82")
        }
        assertThat(admin.run(titles, database = "films")).isNotEmpty()

        admin.run("DROP DATABASE films CASCADE ALIASES")

        assertThat(aliases().map { it["name"] to it["database"] })
            .containsExactly("home" to "neo4j")
        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] })
            .containsExactlyInAnyOrder("neo4j", "system")
        // Nothing of the aliases is left in the system graph.
        server.database(SYSTEM).beginTx().use { tx ->
            val names = tx.findNodes(TopologyGraphDbmsModel.DATABASE_NAME_LABEL).stream().toList()
            assertThat(
                    names.filter {
                        !it.hasRelationship(TopologyGraphDbmsModel.TARGETS_RELATIONSHIP)
                    }
                )
                .isEmpty()
            assertThat(tx.findNodes(TopologyGraphDbmsModel.ALIAS_PROPERTIES_LABEL).stream().count())
                .isEqualTo(1L)
        }
        admin.run("CREATE DATABASE films WAIT")
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        assertThat(admin.run(titles, database = "movies")).isEmpty()
    }

    @Test
    fun `takes an alias for its database in database commands`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run("CREATE ALIAS main FOR DATABASE neo4j")

        assertThat(admin.run("STOP DATABASE movies WAIT").map { it["success"] }).containsOnly(true)
        assertThat(admin.run("SHOW DATABASE films YIELD currentStatus"))
            .containsExactly(mapOf("currentStatus" to "offline"))
        // Where the store would be opened: the database cannot start, which WAIT reports only if
        // it waits for the database, not for one called like the alias.
        val store = server.home.resolve("data/databases/films")
        store.toFile().deleteRecursively()
        Files.createFile(store)
        val started = admin.run("START DATABASE movies WAIT").single()
        assertThat(started).containsEntry("state", "Failed").containsEntry("success", false)
        assertThat(started["message"] as String).contains("Unable to start", "films")

        // The alias itself targets the database.
        assertThat(admin.refused("DROP DATABASE movies"))
            .hasMessage(
                "Failed to delete the specified database 'movies': Database 'films' has " +
                    "aliases 'movies'. Drop them before the database, or drop the database with " +
                    "CASCADE ALIASES."
            )
        assertThat(admin.refused("DROP DATABASE main CASCADE ALIASES"))
            .hasMessage("Not allowed to delete the default database 'neo4j'.")
        assertThat(admin.refused("STOP DATABASE missing"))
            .hasMessage("Failed to stop the specified database 'missing': Database does not exist.")

        assertThat(admin.run("DROP DATABASE movies CASCADE ALIASES WAIT").map { it["success"] })
            .containsOnly(true)
        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] })
            .containsExactlyInAnyOrder("neo4j", "system")
        assertThat(aliases().map { it["name"] }).containsExactly("main")
    }

    @Test
    fun `checks the privileges of the database of an alias, not of its name`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        admin.run("CREATE ROLE operator")
        admin.run("GRANT ROLE operator TO alice")
        val alice = driver("alice", "alice-secret-1")

        assertThat(alice.refused("STOP DATABASE movies"))
            .hasMessageStartingWith("Permission has not been granted for STOP DATABASE.")
        admin.run("GRANT STOP ON DATABASE films TO operator")
        alice.run("STOP DATABASE movies")
        assertThat(admin.run("SHOW DATABASE films YIELD requestedStatus"))
            .containsExactly(mapOf("requestedStatus" to "offline"))
        assertThat(alice.refused("START DATABASE movies"))
            .hasMessageStartingWith("Permission has not been granted for START DATABASE.")

        // Privileges are granted on the database, which the message names.
        val alias =
            "`movies` is an alias of database `films`: privileges are granted on the database."
        val granted = admin.refused("GRANT START ON DATABASE movies TO operator")
        assertThat(granted).hasMessage("Failed to grant privilege to role 'operator': $alias")
        assertThat(granted.gqlStatus()).isEqualTo("42002")
        assertThat(admin.refused("DENY ACCESS ON DATABASE movies TO operator"))
            .hasMessage("Failed to deny privilege to role 'operator': $alias")
        assertThat(admin.refused("REVOKE STOP ON DATABASE movies FROM operator"))
            .hasMessage("Failed to revoke privilege from role 'operator': $alias")
    }

    @Test
    fun `resolves a home database set to an alias`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        admin.run(
            "CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED " +
                "SET HOME DATABASE movies"
        )
        val alice = driver("alice", "alice-secret-1")
        fun home() =
            alice.session().use { session ->
                session.run("CALL db.info() YIELD name").single()["name"].asString()
            }

        // PUBLIC has access to the home database.
        assertThat(home()).isEqualTo("films")
        assertThat(alice.run("SHOW HOME DATABASE YIELD name"))
            .containsExactly(mapOf("name" to "films"))
        assertThat(admin.run("SHOW USERS YIELD user, home WHERE user = 'alice'"))
            .containsExactly(mapOf("user" to "alice", "home" to "movies"))

        admin.run("ALTER ALIAS movies SET DATABASE TARGET neo4j")

        assertThat(home()).isEqualTo("neo4j")
    }

    @Test
    fun `refuses database names that aliases have`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")

        for (command in listOf("CREATE DATABASE movies", "CREATE OR REPLACE DATABASE movies")) {
            val error = admin.refused(command)
            assertThat(error)
                .describedAs(command)
                .hasMessage(
                    "Failed to create the specified database 'movies': A database alias with " +
                        "that name already exists."
                )
            assertThat(error.code()).isEqualTo("Neo.ClientError.Database.ExistingDatabaseFound")
            assertThat(error.gqlStatus()).isEqualTo("42N11")
        }
        val exists = admin.refused("CREATE DATABASE films")
        assertThat(exists)
            .hasMessage("Failed to create the specified database 'films': Database already exists.")
        assertThat(exists.gqlStatus()).isEqualTo("42N11")
        admin.run("CREATE DATABASE movies IF NOT EXISTS")

        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] })
            .doesNotContain("movies")
        assertThat(aliases().map { it["name"] to it["database"] })
            .containsExactly("movies" to "films")
    }

    @Test
    fun `refuses names that are taken, and aliases that do not exist`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")

        for ((name, reason) in
            listOf(
                "movies" to "Database alias already exists.",
                "neo4j" to "A database with that name already exists.",
                "films" to "A database with that name already exists.",
            )) {
            val error = admin.refused("CREATE ALIAS $name FOR DATABASE films")
            assertThat(error)
                .hasMessage("Failed to create the specified database alias '$name': $reason")
            assertThat(error.code()).isEqualTo("Neo.ClientError.Database.ExistingDatabaseFound")
            assertThat(error.gqlStatus()).isEqualTo("42N11")
        }
        // Replacing an alias never replaces a database.
        assertThat(admin.refused("CREATE OR REPLACE ALIAS neo4j FOR DATABASE films"))
            .hasMessage(
                "Failed to create the specified database alias 'neo4j': A database with that " +
                    "name already exists."
            )
        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] })
            .contains("neo4j", "films")

        // A database's own name is not an alias.
        for (name in listOf("missing", "films")) {
            assertThat(admin.refused("DROP ALIAS $name FOR DATABASE"))
                .hasMessage(
                    "Failed to delete the specified database alias '$name': " +
                        "Database alias does not exist."
                )
        }
        assertThat(admin.run("SHOW DATABASES YIELD name").map { it["name"] }).contains("films")
    }

    @Test
    fun `refuses targets other than databases`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        val failed = "Failed to create the specified database alias 'classics'"

        val missing = admin.refused("CREATE ALIAS classics FOR DATABASE nothing")
        assertThat(missing).hasMessage("$failed: Database 'nothing' does not exist.")
        assertThat(missing.code()).isEqualTo("Neo.ClientError.Database.DatabaseNotFound")
        assertThat(admin.refused("CREATE ALIAS classics FOR DATABASE system"))
            .hasMessage(
                "$failed: Target database name 'system' is invalid, due to the prefix 'system'."
            )
        assertThat(admin.refused("CREATE ALIAS classics FOR DATABASE movies"))
            .hasMessage("$failed: 'movies' is an alias, and aliases can only target databases.")
        assertThat(aliases().map { it["name"] }).containsExactly("movies")
    }

    @Test
    fun `refuses invalid names`() {
        assertThat(admin.refused("CREATE ALIAS systemfilms FOR DATABASE films"))
            .hasMessage("Alias name 'systemfilms' is invalid, due to the prefix 'system'.")
        assertThat(admin.refused("CREATE ALIAS _films FOR DATABASE films"))
            .hasMessage("Alias name '_films' is invalid, due to the prefix '_'.")
        assertThat(admin.refused("CREATE ALIAS `films.` FOR DATABASE films"))
            .hasMessage("Alias name 'films.' is invalid: alias names cannot end with a dot.")
        assertThat(admin.refused("CREATE ALIAS `${"f".repeat(65535)}` FOR DATABASE films"))
            .hasMessage("The provided alias is to long, maximum characters are 65534.")
        assertThat(admin.refused("CREATE ALIAS films.old.ones FOR DATABASE films"))
            .hasMessageContaining(
                "'.' is not a valid character in the local alias name 'films.old.ones'."
            )
        assertThat(admin.refused("CREATE OR REPLACE ALIAS movies IF NOT EXISTS FOR DATABASE films"))
            .hasMessageContaining("cannot have both `OR REPLACE` and `IF NOT EXISTS`")
        assertThat(aliases()).isEmpty()
    }

    @Test
    fun `takes dotted names, parameters and properties`() {
        admin.run("CREATE ALIAS films.old FOR DATABASE films")
        admin.run(
            "CREATE ALIAS \$alias FOR DATABASE \$target PROPERTIES \$properties",
            mapOf(
                "alias" to "Movies.Classic",
                "target" to "films",
                "properties" to mapOf("Since" to 1979, "director" to "Ridley Scott"),
            ),
        )

        assertThat(aliases().map { Triple(it["name"], it["composite"], it["properties"]) })
            .containsExactlyInAnyOrder(
                Triple("films.old", null, emptyMap<String, Any>()),
                Triple(
                    "movies.classic",
                    null,
                    mapOf("since" to 1979L, "director" to "Ridley Scott"),
                ),
            )
        assertThat(admin.run("SHOW ALIAS movies.classic FOR DATABASE").map { it["name"] })
            .containsExactly("movies.classic")
        assertThat(admin.run("MATCH (f:Film) RETURN count(f) AS films", database = "films.old"))
            .containsExactly(mapOf("films" to 1L))

        admin.run("DROP ALIAS \$alias FOR DATABASE", mapOf("alias" to "movies.classic"))
        admin.run("DROP ALIAS films.old FOR DATABASE")
        assertThat(aliases()).isEmpty()
    }

    @Test
    fun `refuses properties it cannot keep`() {
        assertThat(
                admin.refused(
                    "CREATE ALIAS movies FOR DATABASE films PROPERTIES \$properties",
                    mapOf("properties" to "rating"),
                )
            )
            .hasMessage(
                "Failed to create the specified database alias 'movies': Invalid properties " +
                    "'String(\"rating\")'. Expected a map value."
            )
        assertThat(admin.refused("CREATE ALIAS movies FOR DATABASE films PROPERTIES {cast: {}}"))
            .hasMessageContaining("Property values can only be of primitive types")
        assertThat(aliases()).isEmpty()
    }

    @Test
    fun `refuses to start with a default database named like an alias`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films")
        drivers.forEach(Driver::close)
        drivers.clear()
        server.stop()
        server =
            StandaloneServer(
                root,
                mapOf(
                    GraphDatabaseSettings.auth_enabled to true,
                    BoltConnector.enabled to true,
                    GraphDatabaseSettings.initial_default_database to "Movies",
                ),
            )

        val failure = catchThrowable { server.start() }

        assertThat(generateSequence(failure) { it.cause }.map { it.message }.toList())
            .contains(
                "Failed to initialize system graph component: The default database 'movies' " +
                    "that initial.dbms.default_database names is a database alias, which cannot " +
                    "be the default database: drop the alias or choose another default database."
            )
    }

    @Test
    fun `keeps aliases over a restart`() {
        admin.run("CREATE ALIAS movies FOR DATABASE films PROPERTIES {rating: 5}")
        drivers.forEach(Driver::close)
        drivers.clear()

        server.restart()

        val admin = driver("neo4j", "neo4j-secret-1")
        assertThat(admin.run("SHOW ALIASES FOR DATABASE YIELD name, database, properties"))
            .containsExactly(
                mapOf(
                    "name" to "movies",
                    "database" to "films",
                    "properties" to mapOf("rating" to 5L),
                )
            )
        assertThat(admin.run("MATCH (f:Film) RETURN count(f) AS films", database = "movies"))
            .containsExactly(mapOf("films" to 1L))
    }

    @Test
    fun `manages and shows aliases with the alias privileges`() {
        admin.run("CREATE USER alice SET PASSWORD 'alice-secret-1' CHANGE NOT REQUIRED")
        admin.run("CREATE ROLE curator")
        admin.run("GRANT ROLE curator TO alice")
        val alice = driver("alice", "alice-secret-1")

        assertThat(alice.refused("CREATE ALIAS movies FOR DATABASE films"))
            .hasMessageStartingWith("Permission has not been granted for CREATE ALIAS.")
        admin.run("GRANT CREATE ALIAS ON DBMS TO curator")
        alice.run("CREATE ALIAS movies FOR DATABASE films")
        alice.run("CREATE ALIAS home FOR DATABASE neo4j")
        assertThat(alice.refused("CREATE OR REPLACE ALIAS movies FOR DATABASE neo4j"))
            .hasMessageStartingWith("Permission has not been granted for DROP ALIAS.")
        assertThat(alice.refused("SHOW ALIASES FOR DATABASE"))
            .hasMessageStartingWith("Permission has not been granted for SHOW ALIAS.")
        assertThat(alice.refused("DROP ALIAS movies FOR DATABASE"))
            .hasMessageStartingWith("Permission has not been granted for DROP ALIAS.")
        assertThat(alice.refused("ALTER ALIAS movies SET DATABASE TARGET neo4j"))
            .hasMessageStartingWith("Permission has not been granted for ALTER ALIAS.")

        // Only the databases alice may access show as targets: PUBLIC has the default one.
        admin.run("GRANT SHOW ALIAS ON DBMS TO curator")
        assertThat(alice.run("SHOW ALIASES FOR DATABASE").map { it["name"] to it["database"] })
            .containsExactlyInAnyOrder("home" to "neo4j", "movies" to null)

        // Dropping a database with its aliases takes dropping aliases too.
        admin.run("GRANT DROP DATABASE ON DBMS TO curator")
        assertThat(alice.refused("DROP DATABASE films CASCADE ALIASES"))
            .hasMessageStartingWith("Permission has not been granted for DROP ALIAS.")

        admin.run("GRANT ALIAS MANAGEMENT ON DBMS TO curator")
        alice.run("DROP ALIAS movies FOR DATABASE")
        assertThat(aliases().map { it["name"] }).containsExactly("home")
    }
}
