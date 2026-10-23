package org.dozendb.core.security

import java.nio.file.Files
import java.nio.file.Path
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.dozendb.core.StandaloneServer
import org.dozendb.core.logging.SecurityLog
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.CleanupMode
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.configuration.connectors.BoltConnector
import org.neo4j.driver.AuthToken
import org.neo4j.driver.AuthTokens
import org.neo4j.driver.GraphDatabase
import org.neo4j.driver.exceptions.AuthenticationException
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.kernel.api.security.AuthToken as Neo4jAuthToken
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException
import org.neo4j.logging.NullLog
import org.neo4j.server.security.auth.BasicLoginContext

/** Logins of other cluster members on the routing connector, refused outside a cluster. */
class MemberLoginsTest {
    // Kept when a test fails, for the server's logs.
    @TempDir(cleanup = CleanupMode.ON_SUCCESS) lateinit var root: Path

    private val connection = ClientConnectionInfo.EMBEDDED_CONNECTION

    private fun token(vararg entries: Pair<String, Any>): MutableMap<String, Any> =
        mutableMapOf(*entries)

    @Test
    fun `a member names the user once this server has its system transaction`() {
        val calls = mutableListOf<String>()
        val realm =
            MemberRealm(
                UserLogins(
                    { user, connection ->
                        calls += "login $user"
                        BasicLoginContext(null, AuthenticationResult.SUCCESS, connection)
                    },
                    { transaction -> calls += "await $transaction" },
                    SecurityLog(NullLog.getInstance(), true),
                )
            )

        val login =
            realm.login(
                token(
                    Neo4jAuthToken.SCHEME_KEY to MemberLogins.SCHEME,
                    Neo4jAuthToken.PRINCIPAL to "alice",
                    Neo4jAuthToken.CREDENTIALS to "",
                    MemberLogins.SYSTEM_TRANSACTION to 42L,
                ),
                connection,
            )

        assertThat(login).isInstanceOf(ForwardedLogin::class.java)
        assertThat(calls).containsExactly("await 42", "login alice")
    }

    @Test
    fun `a user that cannot log in gets no forwarded login`() {
        val realm =
            MemberRealm(
                UserLogins(
                    { _, connection ->
                        BasicLoginContext(null, AuthenticationResult.FAILURE, connection)
                    },
                    {},
                    SecurityLog(NullLog.getInstance(), true),
                )
            )

        val login =
            realm.login(
                token(
                    Neo4jAuthToken.SCHEME_KEY to MemberLogins.SCHEME,
                    Neo4jAuthToken.PRINCIPAL to "mallory",
                ),
                connection,
            )

        assertThat(login).isNotInstanceOf(ForwardedLogin::class.java)
        assertThat(login.subject().authenticationResult).isEqualTo(AuthenticationResult.FAILURE)
    }

    @Test
    fun `without authentication a member's login has full access`() {
        val login =
            MemberRealm(null)
                .login(token(Neo4jAuthToken.SCHEME_KEY to MemberLogins.SCHEME), connection)

        assertThat(login).isInstanceOf(ForwardedLogin::class.java)
        assertThat(login.subject()).isEqualTo(AuthSubject.AUTH_DISABLED)
    }

    @Test
    fun `only members log in`() {
        for (realm in listOf(MemberRealm(null), RefusingMemberRealm("no reason"))) {
            assertThatThrownBy {
                    realm.login(
                        token(
                            Neo4jAuthToken.SCHEME_KEY to "basic",
                            Neo4jAuthToken.PRINCIPAL to "alice",
                            Neo4jAuthToken.CREDENTIALS to "alice-secret-1",
                        ),
                        connection,
                    )
                }
                .isInstanceOf(InvalidAuthTokenException::class.java)
                .hasMessageContaining("the routing connector takes logins of cluster members only")
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = [true, false])
    fun `a server outside a cluster refuses every login on its routing connector`(auth: Boolean) {
        val server =
            StandaloneServer(
                    root,
                    mapOf(
                        GraphDatabaseSettings.auth_enabled to auth,
                        BoltConnector.enabled to true,
                    ),
                )
                .also { it.start() }
        server.use {
            for (token in
                listOf<AuthToken>(
                    AuthTokens.basic("neo4j", "neo4j"),
                    AuthTokens.none(),
                    AuthTokens.custom("neo4j", "", null, MemberLogins.SCHEME),
                )) {
                GraphDatabase.driver("bolt://${server.routingAddress()}", token).use { driver ->
                    assertThatThrownBy { driver.verifyConnectivity() }
                        .isInstanceOf(AuthenticationException::class.java)
                        .hasMessageContaining(
                            "the routing connector takes logins of cluster members only"
                        )
                }
            }
        }
        assertThat(Files.readString(server.home.resolve("logs/debug.log")))
            .doesNotContain("NullPointerException")
    }
}
