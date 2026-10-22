/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-10-23: Neo4j Community's AuthProcedures, with the roles of the current user.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.dozendb.core.security

import java.util.stream.Stream
import org.neo4j.internal.kernel.api.security.SecurityContext
import org.neo4j.kernel.api.procedure.SystemProcedure
import org.neo4j.procedure.Context
import org.neo4j.procedure.Description
import org.neo4j.procedure.Mode
import org.neo4j.procedure.Procedure

/** `dbms.showCurrentUser()`, which shows the roles the current user has, PUBLIC last. */
class DozenDbAuthProcedures {
    @JvmField @Context var securityContext: SecurityContext? = null

    @SystemProcedure
    @Description("Show the current user.")
    @Procedure(name = "dbms.showCurrentUser", mode = Mode.DBMS)
    fun showCurrentUser(): Stream<UserResult> {
        val context = checkNotNull(securityContext)
        val roles = context.roles()
        val ordered =
            roles.filter { it != BuiltInRoles.PUBLIC }.sorted() +
                roles.filter { it == BuiltInRoles.PUBLIC }
        return Stream.of(UserResult(context.subject().executingUser(), ordered, emptyList()))
    }

    class UserResult(
        @JvmField @Description("The name of the current user.") val username: String,
        @JvmField @Description("The roles assigned to the current user.") val roles: List<String>,
        @JvmField @Description("The flags set on the current user.") val flags: List<String>,
    )
}
