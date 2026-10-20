package org.dozendb.core.security

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import org.neo4j.dbms.systemgraph.SystemDatabaseProvider
import org.neo4j.graphdb.Transaction
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_HOME_DB
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_LABEL
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_NAME
import org.neo4j.server.security.systemgraph.versions.KnownCommunitySecurityComponentVersion.USER_SUSPENDED

/**
 * Which users are suspended and which have a home database, as the system graph records them. Read
 * once and kept until [clear], which runs after every change to the system graph.
 */
class SystemGraphUsers(private val systemDatabase: SystemDatabaseProvider) {
    private class Users(val suspended: Set<String>, val homeDatabases: Map<String, String>)

    private class Cached(val generation: Long, val users: Users)

    private val generation = AtomicLong()
    private val cached = AtomicReference<Cached?>()

    fun isSuspended(user: String): Boolean = user in users().suspended

    /** The home database of [user], or null for the DBMS default database. */
    fun homeDatabase(user: String): String? = users().homeDatabases[user]

    /** Forgets what was read: the system graph changed. */
    fun clear() {
        generation.incrementAndGet()
    }

    private fun users(): Users {
        // A read that overlaps a clear() keeps the generation from before it, so is read again.
        val current = generation.get()
        val last = cached.get()
        if (last != null && last.generation == current) return last.users
        val users =
            try {
                systemDatabase.query(::read)
            } catch (e: RuntimeException) {
                // As while a cluster member replaces system with a copy: keep what was read last.
                return last?.users ?: throw e
            }
        cached.set(Cached(current, users))
        return users
    }

    private fun read(transaction: Transaction): Users {
        val suspended = mutableSetOf<String>()
        val homeDatabases = mutableMapOf<String, String>()
        transaction.findNodes(USER_LABEL).use { users ->
            for (user in users) {
                val name = user.getProperty(USER_NAME) as String
                if (user.getProperty(USER_SUSPENDED, false) == true) suspended += name
                (user.getProperty(USER_HOME_DB, null) as String?)?.let { homeDatabases[name] = it }
            }
        }
        return Users(suspended, homeDatabases)
    }
}
