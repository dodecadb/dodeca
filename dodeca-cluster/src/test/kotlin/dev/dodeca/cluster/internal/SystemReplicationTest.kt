package dev.dodeca.cluster.internal

import java.nio.file.Path
import java.time.Duration
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.awaitility.kotlin.atMost
import org.awaitility.kotlin.await
import org.awaitility.kotlin.untilAsserted
import org.awaitility.kotlin.withPollInterval
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.neo4j.graphdb.QueryExecutionException
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.storageengine.api.TransactionIdStore

/** The system database is replicated: administration commands run on its leader, reach everyone. */
class SystemReplicationTest {
    // Kept when a test fails, for the members' logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path
    private var harness: ClusterHarness? = null

    @AfterEach
    fun stop() {
        harness?.close()
    }

    private fun cluster(): ClusterHarness =
        ClusterHarness(root, 3).also {
            harness = it
            it.start()
        }

    private fun ClusterHarness.administer(command: String) {
        awaitLeader(SYSTEM).database(SYSTEM).executeTransactionally(command)
    }

    private fun ClusterHarness.Member.users(): List<String> =
        database(SYSTEM).executeTransactionally("SHOW USERS YIELD user", emptyMap()) {
            it.stream().map { row -> row["user"] as String }.toList()
        }

    private fun ClusterHarness.Member.rolesWithUsers(): List<Pair<String, String?>> =
        database(SYSTEM).executeTransactionally("SHOW ROLES WITH USERS", emptyMap()) {
            it.stream().map { row -> row["role"] as String to row["member"] as String? }.toList()
        }

    private fun ClusterHarness.Member.lastSystemTransaction(): Pair<Long, Long> {
        val transaction =
            database(SYSTEM)
                .dependencyResolver
                .resolveDependency(TransactionIdStore::class.java)
                .lastCommittedTransaction
        return transaction.id() to transaction.consensusIndex()
    }

    private fun ClusterHarness.awaitUsersEverywhere(vararg expected: String) {
        await withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilAsserted
            {
                val leader = awaitLeader(SYSTEM)
                for (member in running()) {
                    assertThat(member.users()).describedAs("$member").contains(*expected)
                    assertThat(member.lastSystemTransaction())
                        .describedAs("$member last system transaction id and Raft index")
                        .isEqualTo(leader.lastSystemTransaction())
                }
            }
    }

    @Test
    fun `a user created on the system leader reaches every member`() {
        val cluster = cluster()

        cluster.administer("CREATE USER alice SET PASSWORD 'alice-secret' CHANGE NOT REQUIRED")

        cluster.awaitUsersEverywhere("alice")
    }

    @Test
    fun `a user altered on the system leader changes on every member`() {
        val cluster = cluster()
        cluster.administer("CREATE USER dave SET PASSWORD 'dave-secret' CHANGE NOT REQUIRED")

        cluster.administer("ALTER USER dave SET PASSWORD 'dave-changed' CHANGE REQUIRED")

        await withPollInterval
            Duration.ofMillis(200) atMost
            Duration.ofSeconds(30) untilAsserted
            {
                for (member in cluster.running()) {
                    assertThat(member.passwordChangeRequired("dave"))
                        .describedAs("$member")
                        .isTrue()
                }
            }
    }

    private fun ClusterHarness.Member.passwordChangeRequired(user: String): Boolean =
        database(SYSTEM).executeTransactionally(
            "SHOW USERS YIELD user, passwordChangeRequired WHERE user = \$user " +
                "RETURN passwordChangeRequired",
            mapOf("user" to user),
        ) {
            it.next()["passwordChangeRequired"] as Boolean
        }

    @Test
    fun `followers refuse administration commands`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val follower = cluster.members.first { it != leader }

        for (command in
            listOf(
                "CREATE USER bob SET PASSWORD 'bob-secret' CHANGE NOT REQUIRED",
                "CREATE ROLE curator",
                "DROP ROLE reader",
                "GRANT ROLE reader TO neo4j",
                "REVOKE ROLE admin FROM neo4j",
                "GRANT CREATE ROLE ON DBMS TO reader",
                "REVOKE ACCESS ON DATABASE * FROM reader",
            )) {
            assertThatThrownBy { follower.database(SYSTEM).executeTransactionally(command) }
                .describedAs(command)
                .isInstanceOf(QueryExecutionException::class.java)
                .hasMessageContaining("must be executed on the LEADER")
                .matches {
                    (it as QueryExecutionException).statusCode ==
                        Status.Cluster.NotALeader.code().serialize()
                }
        }
        assertThat(follower.users()).doesNotContain("bob")
        assertThat(follower.rolesWithUsers())
            .contains("admin" to "neo4j")
            .doesNotContain("reader" to "neo4j")
            .extracting<String> { it.first }
            .contains("reader")
            .doesNotContain("curator")
    }

    @Test
    fun `a member that was down applies the administration commands it missed`() {
        val cluster = cluster()
        val leader = cluster.awaitLeader(SYSTEM)
        val down = cluster.members.first { it != leader }
        down.stop()

        cluster.administer("CREATE USER carol SET PASSWORD 'carol-secret' CHANGE NOT REQUIRED")
        down.start()

        cluster.awaitUsersEverywhere("carol")
    }

    private companion object {
        const val SYSTEM = "system"
    }
}
