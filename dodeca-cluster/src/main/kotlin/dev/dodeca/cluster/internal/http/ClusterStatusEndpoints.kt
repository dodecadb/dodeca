package dev.dodeca.cluster.internal.http

import javax.ws.rs.GET
import javax.ws.rs.Path
import javax.ws.rs.PathParam
import javax.ws.rs.Produces
import javax.ws.rs.core.Context
import javax.ws.rs.core.MediaType
import javax.ws.rs.core.Response
import org.neo4j.kernel.database.NormalizedDatabaseName

/**
 * Neo4j Enterprise's cluster status endpoints of a database, mounted at `/db`: `writable` on the
 * member that leads it, `read-only` on the other members that run it, `available` on every member
 * that runs it, each `true` with 200 OK and else `false` with 404 Not Found, and `status` as JSON
 * on every member that hosts it. A database this member does not host is 404 Not Found.
 *
 * Jersey injects [statuses] into the constructor: `@param:` keeps Kotlin from annotating the final
 * field as well, which HK2 refuses to inject.
 */
@Path("/{databaseName}/cluster")
internal class DatabaseClusterEndpoints(@param:Context private val statuses: ClusterStatuses) {
    @GET
    @Path("/writable")
    @Produces(MediaType.TEXT_PLAIN)
    fun writable(@PathParam("databaseName") database: String): Response =
        answer(of(database)?.writable == true)

    @GET
    @Path("/read-only")
    @Produces(MediaType.TEXT_PLAIN)
    fun readOnly(@PathParam("databaseName") database: String): Response =
        answer(of(database)?.let { it.running && !it.writable } == true)

    @GET
    @Path("/available")
    @Produces(MediaType.TEXT_PLAIN)
    fun available(@PathParam("databaseName") database: String): Response =
        answer(of(database)?.running == true)

    @GET
    @Path("/status")
    @Produces(MediaType.APPLICATION_JSON)
    fun status(@PathParam("databaseName") database: String): Response =
        of(database)?.let { Response.ok(it.json()).build() }
            ?: Response.status(Response.Status.NOT_FOUND).build()

    private fun of(database: String): DatabaseClusterStatus? {
        val name = NormalizedDatabaseName(database).name()
        return statuses.statuses().firstOrNull { it.name == name }
    }

    private fun answer(yes: Boolean): Response =
        Response.status(if (yes) Response.Status.OK else Response.Status.NOT_FOUND)
            .entity(yes.toString())
            .build()
}

/** Neo4j Enterprise's `/dbms/cluster/status`: every database this member hosts, as JSON. */
@Path("/cluster/status")
internal class DbmsClusterEndpoints(@param:Context private val statuses: ClusterStatuses) {
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    fun status(): Response =
        Response.ok(Json.array(statuses.statuses().map { it.dbmsJson() })).build()
}
