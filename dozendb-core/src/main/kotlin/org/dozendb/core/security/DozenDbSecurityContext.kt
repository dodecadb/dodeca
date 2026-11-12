package org.dozendb.core.security

import java.util.UUID
import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.AccessMode
import org.neo4j.internal.kernel.api.security.AdminAccessMode
import org.neo4j.internal.kernel.api.security.AdminActionOnResource
import org.neo4j.internal.kernel.api.security.AuthSubject
import org.neo4j.internal.kernel.api.security.DatabaseAccessMode
import org.neo4j.internal.kernel.api.security.LoginContext
import org.neo4j.internal.kernel.api.security.PermissionState
import org.neo4j.internal.kernel.api.security.PrivilegeAction
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.internal.kernel.api.security.Segment
import org.neo4j.internal.kernel.api.security.UserSegment
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NormalizedDatabaseName
import org.neo4j.kernel.database.PrivilegeDatabaseReference

/**
 * What a user may do in one transaction of the database [database], from their privileges.
 *
 * Neo4j's own [SecurityContext] allows every administration action, and loses the restrictions a
 * subclass adds in [withMode] and [authorize]; every one of its methods is overridden here so that
 * none is lost.
 */
class DozenDbSecurityContext
internal constructor(
    subject: AuthSubject,
    mode: AccessMode,
    connectionInfo: ClientConnectionInfo,
    database: String,
    private val user: Authorization,
    private val adminMode: AdminAccessMode,
) : SecurityContext(subject, mode, connectionInfo, database) {
    /**
     * One user's privileges in the [database] that a transaction is in, as one read of the system
     * graph has them, and the [login] that authorizes their transactions in other databases.
     */
    internal class Authorization(
        val database: String,
        val policy: PrivilegePolicy,
        val roles: Set<String>,
        val graph: SecurityGraph,
        val login: LoginContext,
        val accessMode: DozenDbAccessMode,
    ) : AdminAccessMode, DatabaseAccessMode {
        /** Administration actions, as [PrivilegePolicy.admin] decides them. */
        override fun allows(action: AdminActionOnResource): PermissionState {
            val request = AdminRequest.of(action) ?: return PermissionState.NOT_GRANTED
            if (request.action in GRAPH_ACTIONS) return PermissionState.NOT_GRANTED
            return policy.admin(
                request.action,
                request.database?.let(::databaseId),
                request.user?.let(PrivilegeSegment::Users) ?: PrivilegeSegment.Database,
            )
        }

        /**
         * SHOW DATABASES: `system`, those with ACCESS, all for database management, and those ALTER
         * DATABASE or SET DATABASE ACCESS is granted on, as Neo4j Enterprise 2025.08 has it in
         * Cypher 25; SET DATABASE DEFAULT LANGUAGE shows none.
         */
        override fun canSeeDatabase(database: DatabaseReference): Boolean =
            canAccessDatabase(database) ||
                seesAll ||
                SEE_ONE.any {
                    policy.admin(it, databaseId(database.owningDatabaseName())).allowsAccess()
                }

        override fun canAccessDatabase(database: DatabaseReference): Boolean {
            val name = database.owningDatabaseName()
            return name == GraphDatabaseSettings.SYSTEM_DATABASE_NAME ||
                policy.access(databaseId(name)).allowsAccess()
        }

        private val seesAll: Boolean by lazy {
            SEE_ALL.any { policy.admin(it, null).allowsAccess() }
        }

        /** The id of the database [name], or one of no database if there is none. */
        fun databaseId(name: String): UUID =
            graph.databaseId(NormalizedDatabaseName(name).name()) ?: NO_DATABASE
    }

    override fun databaseAccessMode(): DatabaseAccessMode = user

    override fun allowExecuteAdminProcedure(procedureId: Int): PermissionState =
        user.accessMode.allowExecuteAdminProcedure(procedureId)

    override fun allowsAdminAction(action: AdminActionOnResource): PermissionState =
        adminMode.allows(action)

    override fun roles(): Set<String> = user.roles

    /**
     * This context in its own database, as for transactions that procedures open there; another
     * database's own, without what this transaction was given for a procedure.
     */
    override fun authorize(
        idLookup: IdLookup,
        dbReference: PrivilegeDatabaseReference,
        securityLog: AbstractSecurityLog,
        timeOfEvaluationMillis: Long,
    ): SecurityContext =
        if (NormalizedDatabaseName(owner(dbReference)).name() == user.database) {
            this
        } else {
            user.login.authorize(idLookup, dbReference, securityLog, timeOfEvaluationMillis)
        }

    override fun withMode(mode: AccessMode): DozenDbSecurityContext =
        DozenDbSecurityContext(subject(), mode, connectionInfo(), database(), user, adminMode)

    override fun withMode(adminAccessMode: AdminAccessMode): DozenDbSecurityContext =
        DozenDbSecurityContext(
            subject(),
            mode(),
            connectionInfo(),
            database(),
            user,
            adminAccessMode,
        )

    override fun isAuthDisabled(): Boolean = false

    /**
     * The login's OIDC token, which Neo4j Enterprise forwards to remote aliases; DozenDB has none.
     */
    override fun oidcToken(): String? = user.login.oidcToken()

    /** This context: the access to other shards of a sharded database, which DozenDB has not. */
    override fun withExternalShardAccess(): DozenDbSecurityContext = this

    override fun toString(): String = defaultString("dozendb")

    /** The parts of an [AdminActionOnResource], which Neo4j keeps private. */
    internal class AdminRequest(
        val action: PrivilegeAction,
        val database: String?,
        val user: String?,
    ) {
        companion object {
            private val ACTION = field(AdminActionOnResource::class.java, "action")
            private val SCOPE = field(AdminActionOnResource::class.java, "databaseScope")
            private val SEGMENT = field(AdminActionOnResource::class.java, "segment")
            private val SCOPE_NAME = field(AdminActionOnResource.DatabaseScope::class.java, "name")
            private val SCOPE_ALL = field(AdminActionOnResource.DatabaseScope::class.java, "all")

            /** The parts of [request]; null for a segment other than a database's or user's. */
            fun of(request: AdminActionOnResource): AdminRequest? {
                val scope = SCOPE.get(request)
                val user =
                    when (val segment = SEGMENT.get(request)) {
                        Segment.ALL -> null
                        is UserSegment -> segment.username() ?: return null
                        else -> return null
                    }
                return AdminRequest(
                    ACTION.get(request) as PrivilegeAction,
                    if (SCOPE_ALL.getBoolean(scope)) null else SCOPE_NAME.get(scope) as String,
                    user,
                )
            }

            private fun field(type: Class<*>, name: String) =
                type.getDeclaredField(name).apply { isAccessible = true }
        }
    }

    private companion object {
        /**
         * The DBMS privileges that show every standard database in SHOW DATABASES; ALTER DATABASE
         * and SET DATABASE ACCESS on the DBMS do as on all databases.
         */
        val SEE_ALL = listOf(PrivilegeAction.CREATE_DATABASE, PrivilegeAction.DROP_DATABASE)

        /** The privileges that show the databases they are on in SHOW DATABASES. */
        val SEE_ONE = listOf(PrivilegeAction.ALTER_DATABASE, PrivilegeAction.SET_DATABASE_ACCESS)
    }
}
