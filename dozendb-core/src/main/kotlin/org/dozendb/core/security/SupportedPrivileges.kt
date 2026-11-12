package org.dozendb.core.security

/**
 * What SHOW SUPPORTED PRIVILEGES lists: every privilege GRANT and DENY take, named as their
 * commands name them. Neo4j's docs give ten rows, kept verbatim here; the others follow the GRANT
 * syntax of the docs in the same style.
 */
object SupportedPrivileges {
    /**
     * One privilege: its [action], what further limits it ([qualifier]), whether it applies to the
     * DBMS, a database, a graph or data sources ([target]), and the elements it can apply to
     * ([scope]).
     */
    data class Supported(
        val action: String,
        val qualifier: String?,
        val target: String,
        val scope: List<String>?,
        val description: String,
    )

    private val ALL_ELEMENTS = listOf("elements", "nodes", "pattern", "relationships")
    private val ELEMENTS = listOf("elements", "nodes", "relationships")

    private fun graph(
        action: String,
        qualifier: String?,
        scope: List<String>?,
        description: String,
    ) = Supported(action, qualifier, "graph", scope, description)

    private fun database(action: String, description: String, qualifier: String? = null) =
        Supported(action, qualifier, "database", null, description)

    private fun dbms(action: String, description: String, qualifier: String? = null) =
        Supported(action, qualifier, "dbms", null, description)

    val ALL: List<Supported> =
        listOf(
            graph("traverse", null, ALL_ELEMENTS, "enables the specified entities to be found"),
            graph(
                "read",
                "property",
                ALL_ELEMENTS,
                "enables the specified properties of the found entities to be read",
            ),
            graph(
                "match",
                "property",
                ALL_ELEMENTS,
                "enables the specified entities to be found and their specified properties to be read",
            ),
            graph(
                "merge",
                "property",
                ELEMENTS,
                "enables the specified entities to be found, created and their specified properties to be read and set",
            ),
            graph("create", null, ELEMENTS, "enables the specified entities to be created"),
            graph("delete", null, ELEMENTS, "enables the specified entities to be deleted"),
            graph("set label", "label", null, "enables the specified labels to be set on nodes"),
            graph(
                "remove label",
                "label",
                null,
                "enables the specified labels to be removed from nodes",
            ),
            graph(
                "set property",
                "property",
                ELEMENTS,
                "enables the specified properties of the specified entities to be set",
            ),
            graph("write", null, null, "allows all WRITE operations on an entire graph"),
            graph(
                "all graph privileges",
                null,
                null,
                "allows all READ and WRITE operations on an entire graph",
            ),
            database("access", "allows access to the specified database"),
            database("start", "allows the specified database to be started"),
            database("stop", "allows the specified database to be stopped"),
            database("create index", "allows indexes to be created on the specified database"),
            database("drop index", "allows indexes to be deleted on the specified database"),
            database("show index", "allows indexes to be listed on the specified database"),
            database(
                "index management",
                "allows indexes to be created, deleted and listed on the specified database",
            ),
            database(
                "create constraint",
                "allows constraints to be created on the specified database",
            ),
            database(
                "drop constraint",
                "allows constraints to be deleted on the specified database",
            ),
            database(
                "show constraint",
                "allows constraints to be listed on the specified database",
            ),
            database(
                "constraint management",
                "allows constraints to be created, deleted and listed on the specified database",
            ),
            database(
                "create new node label",
                "allows new node labels to be created on the specified database",
            ),
            database(
                "create new relationship type",
                "allows new relationship types to be created on the specified database",
            ),
            database(
                "create new property name",
                "allows new property names to be created on the specified database",
            ),
            database(
                "name management",
                "allows new node labels, relationship types and property names to be created on the specified database",
            ),
            database(
                "all database privileges",
                "allows access, index, constraint and name management on the specified database",
            ),
            database(
                "show transactions",
                "allows listing transactions and queries for the specified users on the specified database",
                "username",
            ),
            database(
                "terminate transactions",
                "allows ending transactions and queries for the specified users on the specified database",
                "username",
            ),
            database(
                "transaction management",
                "allows listing and ending transactions and queries for the specified users on the specified database",
                "username",
            ),
            dbms(
                "all dbms privileges",
                "enables the specified roles to perform all DBMS administration",
            ),
            dbms(
                "role management",
                "enables the specified roles to create, delete, rename, assign, remove and list roles",
            ),
            dbms("create role", "enables the specified roles to create roles"),
            dbms("rename role", "enables the specified roles to rename roles"),
            dbms("drop role", "enables the specified roles to delete roles"),
            dbms("assign role", "enables the specified roles to assign roles to users"),
            dbms("remove role", "enables the specified roles to remove roles from users"),
            dbms("show role", "enables the specified roles to list roles"),
            dbms(
                "user management",
                "enables the specified roles to create, delete, modify, and list users",
            ),
            dbms("create user", "enables the specified roles to create users"),
            dbms("rename user", "enables the specified roles to rename users"),
            dbms("drop user", "enables the specified roles to delete users"),
            dbms("show user", "enables the specified roles to list users"),
            dbms(
                "show user credentials",
                "enables the specified roles to list users as commands, with show user",
            ),
            dbms("alter user", "enables the specified roles to modify users"),
            dbms(
                "user metadata management",
                "enables the specified roles to set, remove, and view attributes and tags on users",
            ),
            dbms(
                "set user metadata",
                "enables the specified roles to set, add, and remove attributes and tags on users",
            ),
            dbms(
                "show user metadata",
                "enables the specified roles to view attributes and tags on users",
            ),
            dbms("set user status", "enables the specified roles to suspend and activate users"),
            dbms("set passwords", "enables the specified roles to set the passwords of users"),
            dbms("set auth", "enables the specified roles to set the authentication of users"),
            dbms(
                "set user home database",
                "enables the specified roles to set the home database of users",
            ),
            dbms(
                "database management",
                "enables the specified roles to create, delete and modify databases",
            ),
            dbms("create database", "enables the specified roles to create databases"),
            dbms("drop database", "enables the specified roles to delete databases"),
            dbms("alter database", "enables the specified roles to modify databases"),
            dbms(
                "set database access",
                "enables the specified roles to set the access mode of databases",
            ),
            dbms(
                "composite database management",
                "enables the specified roles to create and delete composite databases",
            ),
            dbms(
                "create composite database",
                "enables the specified roles to create composite databases",
            ),
            dbms(
                "drop composite database",
                "enables the specified roles to delete composite databases",
            ),
            dbms(
                "alias management",
                "enables the specified roles to create, delete, modify and list aliases",
            ),
            dbms("create alias", "enables the specified roles to create aliases"),
            dbms("drop alias", "enables the specified roles to delete aliases"),
            dbms("alter alias", "enables the specified roles to modify aliases"),
            dbms("show alias", "enables the specified roles to list aliases"),
            dbms(
                "privilege management",
                "enables the specified roles to assign, remove and list privileges",
            ),
            dbms("show privilege", "enables the specified roles to list privileges"),
            dbms("assign privilege", "enables the specified roles to assign privileges"),
            dbms("remove privilege", "enables the specified roles to remove privileges"),
            dbms(
                "server management",
                "enables the specified roles to enable, modify, deallocate, drop and list servers",
            ),
            dbms("show servers", "enables the specified roles to list servers"),
            dbms(
                "impersonate",
                "enables the specified roles to impersonate the specified users",
                "username",
            ),
            dbms(
                "execute procedure",
                "enables the specified roles to execute the given procedures",
                "procedure",
            ),
            dbms(
                "execute boosted procedure",
                "enables the specified roles to execute the given procedures with elevated privileges",
                "procedure",
            ),
            dbms(
                "execute admin procedures",
                "enables the specified roles to execute procedures annotated with @Admin",
            ),
            dbms(
                "execute function",
                "enables the specified roles to execute the given user-defined functions",
                "function",
            ),
            dbms(
                "execute boosted function",
                "enables the specified roles to execute the given user-defined functions with elevated privileges",
                "function",
            ),
            dbms(
                "show settings",
                "enables the specified roles to show the specified configuration settings",
                "setting",
            ),
            Supported(
                "load",
                null,
                "all data",
                null,
                "enables the specified roles to load data from any source with LOAD CSV",
            ),
            Supported(
                "load",
                null,
                "cidr",
                null,
                "enables the specified roles to load data with LOAD CSV from addresses in the given range",
            ),
        )
}
