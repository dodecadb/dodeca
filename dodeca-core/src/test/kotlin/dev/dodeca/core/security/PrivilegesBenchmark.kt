package dev.dodeca.core.security

import dev.dodeca.core.StandaloneServer
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.io.ByteUnit
import org.neo4j.kernel.api.KernelTransaction
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken

/**
 * What privileges cost: starting transactions, and reading a million nodes, as the admin and as
 * users whose roles deny a label or have a property rule. Not part of the suites; run it with
 * `./mvnw -pl dodeca-core test -Dtest=PrivilegesBenchmark` and read what it prints.
 */
class PrivilegesBenchmark {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    @Test
    fun `measure what privileges cost`() {
        StandaloneServer(
                root,
                mapOf(
                    GraphDatabaseSettings.auth_enabled to true,
                    GraphDatabaseSettings.pagecache_memory to ByteUnit.gibiBytes(1),
                ),
            )
            .use { server ->
                server.start()
                setUp(server)
                val database = server.database()
                val auth =
                    server
                        .database(StandaloneServer.SYSTEM)
                        .dependencyResolver
                        .resolveDependency(AuthManager::class.java)
                // Without a login, as embedded code runs: no privileges at all.
                val begins =
                    mapOf<String, () -> Transaction>("embedded" to database::beginTx) +
                        USERS.keys.associateWith { user ->
                            val login =
                                auth.login(
                                    AuthToken.newBasicAuthToken(user, "$user-secret-1"),
                                    ClientConnectionInfo.EMBEDDED_CONNECTION,
                                )
                            val begin: () -> Transaction = {
                                database.beginTransaction(KernelTransaction.Type.EXPLICIT, login)
                            }
                            begin
                        }
                // In turns, so that each is measured as warm as the others.
                begins.values.forEach(::transactions)
                val rates = begins.mapValues { 0.0 }.toMutableMap()
                repeat(ROUNDS) {
                    for ((user, begin) in begins) {
                        rates[user] = maxOf(rates.getValue(user), transactions(begin))
                    }
                }
                val report = StringBuilder()
                report.appendLine(
                    "%-10s %16s %14s %10s".format("as", "transactions/s", "scan ms", "nodes")
                )
                for ((user, begin) in begins) {
                    val (millis, count) = scan(begin)
                    assertThat(count).describedAs(user).isEqualTo(EXPECTED.getValue(user))
                    report.appendLine(
                        "%-10s %16.0f %14d %10d".format(user, rates[user], millis, count)
                    )
                }
                println(report)
            }
    }

    private fun setUp(server: StandaloneServer) {
        server.administer("ALTER USER neo4j SET PASSWORD 'neo4j-secret-1' CHANGE NOT REQUIRED")
        for ((user, grants) in USERS) {
            if (user == "neo4j") continue
            server.administer("CREATE USER $user SET PASSWORD '$user-secret-1' CHANGE NOT REQUIRED")
            server.administer("CREATE ROLE $user")
            server.administer("GRANT ROLE $user TO $user")
            for (grant in grants) server.administer("$grant TO $user")
        }
        for (batch in 0 until NODES / BATCH) {
            server.rows(
                "UNWIND range(\$from, \$to) AS i CREATE (n:Person {age: i % 100}) " +
                    "WITH n, i WHERE i % 10 = 0 SET n:Secret",
                parameters = mapOf("from" to batch * BATCH, "to" to (batch + 1) * BATCH - 1),
            )
        }
    }

    /** Transactions started and committed per second. */
    private fun transactions(begin: () -> Transaction): Double {
        val start = System.nanoTime()
        repeat(TRANSACTIONS) { begin().use(Transaction::commit) }
        return TRANSACTIONS / ((System.nanoTime() - start) / 1e9)
    }

    /** The median time of reading every person's age, after warming up, and the nodes read. */
    private fun scan(begin: () -> Transaction): Pair<Long, Long> {
        var count = 0L
        val times =
            (1..WARMUPS + SCANS).map {
                val start = System.nanoTime()
                begin().use { tx ->
                    count =
                        tx.execute("MATCH (n:Person) WHERE n.age >= 0 RETURN count(*) AS count")
                            .columnAs<Long>("count")
                            .next()
                    tx.commit()
                }
                (System.nanoTime() - start) / 1_000_000
            }
        return times.drop(WARMUPS).sorted()[SCANS / 2] to count
    }

    private companion object {
        const val NODES = 1_000_000
        const val BATCH = 100_000
        const val TRANSACTIONS = 100_000
        const val ROUNDS = 3
        const val WARMUPS = 2
        const val SCANS = 5

        /** The users measured and what their roles grant and deny, besides PUBLIC's. */
        val USERS =
            mapOf(
                "neo4j" to emptyList(),
                "granted" to listOf("GRANT MATCH {*} ON GRAPH neo4j"),
                "denied" to
                    listOf(
                        "GRANT MATCH {*} ON GRAPH neo4j",
                        "DENY MATCH {*} ON GRAPH neo4j NODES Secret",
                    ),
                "ruled" to listOf("GRANT MATCH {*} ON GRAPH neo4j FOR (n) WHERE n.age < 50"),
            )

        val EXPECTED =
            mapOf(
                "embedded" to NODES.toLong(),
                "neo4j" to NODES.toLong(),
                "granted" to NODES.toLong(),
                "denied" to NODES * 9L / 10,
                "ruled" to NODES / 2L,
            )
    }
}
