package dev.dodeca.core.security

import org.neo4j.internal.kernel.api.security.PrivilegeAction

/**
 * The roles a DBMS starts with and their privileges, as Neo4j's docs list them ("Built-in roles and
 * privileges"). Each one has its predecessor's privileges and more.
 */
object BuiltInRoles {
    const val PUBLIC = "PUBLIC"
    const val READER = "reader"
    const val EDITOR = "editor"
    const val PUBLISHER = "publisher"
    const val ARCHITECT = "architect"
    const val ADMIN = "admin"

    /** The roles in the order they are created, with their privileges. */
    val ALL: Map<String, List<Privilege>> by lazy {
        val reader =
            listOf(
                onDatabases(PrivilegeAction.ACCESS),
                match(PrivilegeSegment.Nodes(null)),
                match(PrivilegeSegment.Relationships(null)),
                onDatabases(PrivilegeAction.SHOW_CONSTRAINT),
                onDatabases(PrivilegeAction.SHOW_INDEX),
            )
        val editor =
            reader +
                write(PrivilegeSegment.Nodes(null)) +
                write(PrivilegeSegment.Relationships(null))
        val publisher = editor + onDatabases(PrivilegeAction.TOKEN)
        val architect =
            publisher + onDatabases(PrivilegeAction.CONSTRAINT) + onDatabases(PrivilegeAction.INDEX)
        val admin =
            architect +
                listOf(
                    onDbms(PrivilegeAction.DBMS_ACTIONS),
                    onDbms(PrivilegeAction.LOAD, PrivilegeSegment.AllData),
                    onDatabases(PrivilegeAction.START_DATABASE),
                    onDatabases(PrivilegeAction.STOP_DATABASE),
                    onDatabases(
                        PrivilegeAction.TRANSACTION_MANAGEMENT,
                        PrivilegeSegment.Users(null),
                    ),
                )
        linkedMapOf(
            PUBLIC to
                listOf(
                    grant(PrivilegeAction.ACCESS, PrivilegeScope.Home),
                    onDbms(PrivilegeAction.EXECUTE, PrivilegeSegment.Functions("*")),
                    onDbms(PrivilegeAction.EXECUTE, PrivilegeSegment.Procedures("*")),
                    onDbms(PrivilegeAction.LOAD, PrivilegeSegment.AllData),
                ),
            READER to reader,
            EDITOR to editor,
            PUBLISHER to publisher,
            ARCHITECT to architect,
            ADMIN to admin,
        )
    }

    private fun grant(
        action: PrivilegeAction,
        scope: PrivilegeScope,
        resource: PrivilegeResource = PrivilegeResource.Database,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = Privilege(Access.GRANTED, action, scope, resource, segment)

    private fun onDbms(
        action: PrivilegeAction,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = grant(action, PrivilegeScope.Dbms, segment = segment)

    private fun onDatabases(
        action: PrivilegeAction,
        segment: PrivilegeSegment = PrivilegeSegment.Database,
    ) = grant(action, PrivilegeScope.AllDatabases, segment = segment)

    private fun match(segment: PrivilegeSegment) =
        grant(
            PrivilegeAction.MATCH,
            PrivilegeScope.AllDatabases,
            PrivilegeResource.AllProperties,
            segment,
        )

    private fun write(segment: PrivilegeSegment) =
        grant(PrivilegeAction.WRITE, PrivilegeScope.AllDatabases, PrivilegeResource.Graph, segment)
}
