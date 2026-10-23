package org.dozendb.core.security

import org.dozendb.core.logging.SecurityLog
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AuthenticationResult
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.security.AuthManager
import org.neo4j.kernel.api.security.AuthToken
import org.neo4j.kernel.api.security.exception.InvalidAuthTokenException
import org.neo4j.kernel.database.PrivilegeDatabaseReference

/**
 * Whether other members of a cluster may log in on this server's routing connector
 * (`server.routing.listen_address`) as the users whose transactions they forward.
 */
sealed interface MemberLogins {
    /** They may: the routing connector lets in only clients with a member's certificate. */
    data object Allowed : MemberLogins

    /** They may not, for [reason], which refusals and the log give. */
    data class Refused(val reason: String) : MemberLogins

    companion object {
        /** The authentication scheme of members' logins on the routing connector. */
        const val SCHEME = "dozendb-member"

        /**
         * The token key of the last transaction of `system` that the member had when it forwarded
         * the user's transaction; this server looks the user up once it has that transaction too.
         */
        const val SYSTEM_TRANSACTION = "system_transaction"
    }
}

/**
 * The login of another member on the routing connector, which runs the transactions of the user of
 * [login] that the member forwarded. This server never forwards them again.
 */
class ForwardedLogin(val login: LoginContext) :
    LoginContext(login.subject(), login.connectionInfo()) {
    override fun authorize(
        idLookup: IdLookup,
        dbReference: PrivilegeDatabaseReference,
        securityLog: AbstractSecurityLog,
    ): SecurityContext = login.authorize(idLookup, dbReference, securityLog)

    override fun nativelyAuthenticated(): Boolean = login.nativelyAuthenticated()
}

/**
 * The logins of users on this server, without their passwords: [loginAs] gives a user's login as
 * [AuthManager.login] would with their password, and [awaitSystem] waits until this server has the
 * given transaction of `system`. Failed logins go to the [securityLog].
 */
internal class UserLogins(
    val loginAs: (String, ClientConnectionInfo) -> LoginContext,
    val awaitSystem: (Long) -> Unit,
    val securityLog: SecurityLog,
)

/**
 * Logins on the routing connector, which only members may use: [MemberLogins.SCHEME] tokens naming
 * the user, whose login [users] gives once this server's system graph is as recent as the member's
 * was. With authentication disabled, [users] is null and logins have full access. Members logged
 * their users' logins when they logged in.
 */
internal class MemberRealm(private val users: UserLogins?) : AuthManager() {
    override fun login(
        authToken: MutableMap<String, Any>,
        connectionInfo: ClientConnectionInfo,
    ): LoginContext {
        AuthToken.clearCredentials(authToken)
        val scheme = authToken[AuthToken.SCHEME_KEY]
        if (scheme != MemberLogins.SCHEME) {
            throw InvalidAuthTokenException.unsupportedAuthenticationToken(
                ": the routing connector takes logins of cluster members only, not '$scheme'. " +
                    "Clients connect to server.bolt.listen_address"
            )
        }
        if (users == null) return ForwardedLogin(LoginContext.fullAccess(connectionInfo))
        val user = authToken[AuthToken.PRINCIPAL] as? String ?: ""
        (authToken[MemberLogins.SYSTEM_TRANSACTION] as? Long)?.let(users.awaitSystem)
        val login = users.loginAs(user, connectionInfo)
        val result = login.subject().authenticationResult
        if (
            result != AuthenticationResult.SUCCESS &&
                result != AuthenticationResult.PASSWORD_CHANGE_REQUIRED
        ) {
            users.securityLog.loginFailed(
                user,
                connectionInfo,
                "a member forwarded a transaction of a user who does not exist or is suspended",
            )
            return login
        }
        return ForwardedLogin(login)
    }

    override fun impersonate(originalAuth: LoginContext, userToImpersonate: String): LoginContext {
        val login = (originalAuth as ForwardedLogin).login
        if (users == null) return AuthManager.NO_AUTH.impersonate(login, userToImpersonate)
        return ForwardedLogin(
            (login as DozenDbLoginContext).impersonate(userToImpersonate, users.securityLog)
        )
    }
}

/** Refuses every login on the routing connector, saying why. */
internal class RefusingMemberRealm(private val reason: String) : AuthManager() {
    override fun login(
        authToken: MutableMap<String, Any>,
        connectionInfo: ClientConnectionInfo,
    ): LoginContext {
        AuthToken.clearCredentials(authToken)
        throw InvalidAuthTokenException.unsupportedAuthenticationToken(
            ": the routing connector takes logins of cluster members only, and this server " +
                "takes none: $reason. Clients connect to server.bolt.listen_address"
        )
    }

    override fun impersonate(originalAuth: LoginContext, userToImpersonate: String): LoginContext =
        throw InvalidArgumentsException(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_51N30)
                .withParam(GqlParams.StringParam.item, "Impersonation")
                .withParam(GqlParams.StringParam.context, "the routing connector")
                .build(),
            "Impersonation is not supported on the routing connector.",
        )
}
