package org.dozendb.core

import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.gqlstatus.ErrorGqlStatusObject
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

/**
 * The GQL statuses of DozenDB's errors. Neo4j requires one of every exception, and clients read it
 * as they read Neo4j Enterprise's: DozenDB gives the code Neo4j has for the same error, as Neo4j's
 * status code documentation describes it, with the message saying why as before.
 */
object Gql {
    /** [code], with its [params], caused by [cause]. */
    fun status(
        code: GqlStatusInfoCodes,
        vararg params: Pair<GqlParams.GqlParam, Any>,
        cause: ErrorGqlStatusObject? = null,
    ): ErrorGqlStatusObject {
        val builder = ErrorGqlStatusObjectImplementation.from(code)
        for ((param, value) in params) {
            when (param) {
                is GqlParams.StringParam -> builder.withParam(param, value as String)
                is GqlParams.NumberParam -> builder.withParam(param, value as Number)
                is GqlParams.ListParam -> builder.withParam(param, value as List<*>)
                is GqlParams.BooleanParam -> builder.withParam(param, value as Boolean)
                else -> error("Unknown kind of GQL parameter: $param")
            }
        }
        if (cause != null) builder.withCause(cause)
        return builder.build()
    }

    /** 51N41: a server or database administration operation is not possible, for [reason]. */
    fun notPossible(reason: String): ErrorGqlStatusObject =
        status(GqlStatusInfoCodes.STATUS_51N41, GqlParams.StringParam.msg to reason)

    /**
     * An administration command or procedure call refused for the reason [message] gives, such as
     * "Could not drop server 'a': it is the only server." (51N41).
     */
    fun refused(message: String, cause: Throwable? = null): InvalidArgumentException =
        InvalidArgumentException(notPossible(message), message, cause)

    /**
     * [code], one of Neo4j's about an operation on [server], such as 51N44 "Cannot drop server",
     * caused by [notPossible] for [reason].
     */
    fun server(code: GqlStatusInfoCodes, server: String, reason: String): ErrorGqlStatusObject =
        status(code, GqlParams.StringParam.server to server, cause = notPossible(reason))

    /** 50N00: an internal error, in [component], of [message]. */
    fun internal(component: String, message: String): ErrorGqlStatusObject =
        status(
            GqlStatusInfoCodes.STATUS_50N00,
            GqlParams.StringParam.msgTitle to component,
            GqlParams.StringParam.msg to message,
        )
}
