package org.dozendb.core.cypher

import java.net.URI
import java.net.URISyntaxException
import org.dozendb.core.security.AliasPasswords
import org.neo4j.common.DependencyResolver
import org.neo4j.configuration.Config
import org.neo4j.cypher.internal.`AdministrationCommandRuntime$`
import org.neo4j.cypher.internal.AdministrationCommandRuntimeContext
import org.neo4j.cypher.internal.AliasMapSettingsEvaluator
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ExecutionEngine
import org.neo4j.cypher.internal.ExecutionPlan
import org.neo4j.cypher.internal.ast.DatabaseName
import org.neo4j.cypher.internal.ast.RemoteAliasStoredCredentials
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.logical.plans.AlterRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.CreateRemoteDatabaseAlias
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NameValidator
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.procs.`Continue$`
import org.neo4j.cypher.internal.procs.ParameterTransformer
import org.neo4j.cypher.internal.procs.QueryHandler
import org.neo4j.cypher.internal.procs.ThrowException
import org.neo4j.cypher.internal.procs.UpdatingSystemCommandExecutionPlan
import org.neo4j.dbms.systemgraph.TopologyGraphDbmsModel
import org.neo4j.exceptions.DatabaseAdministrationOnFollowerException
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.Transaction
import org.neo4j.internal.kernel.api.security.AbstractSecurityLog
import org.neo4j.internal.kernel.api.security.SecurityAuthorizationHandler
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.values.storable.BooleanValue
import org.neo4j.values.storable.TextValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.MapValue
import org.neo4j.values.virtual.MapValueBuilder
import org.neo4j.values.virtual.VirtualValues
import scala.Function2
import scala.Function3
import scala.Option
import scala.PartialFunction
import scala.Tuple2
import scala.collection.immutable.Map as ScalaMap
import scala.collection.immutable.`Set$`
import scala.jdk.javaapi.CollectionConverters
import scala.util.Either

/**
 * CREATE and ALTER ALIAS for remote aliases: names, in this DBMS, of databases of other DBMSs,
 * which queries reach with the credentials the alias keeps. A remote alias is a `DatabaseName` node
 * labelled `Remote` in `system`, without a database, as Neo4j models it: the `url` of the other
 * DBMS, the `target_name` of the database there, the `username`, the `password` that
 * [AliasPasswords] encrypted with its `iv`, and a `version` that changes with any of them, so that
 * connections are made anew. Its driver settings and properties are in `DriverSettings` and
 * `AliasProperties` nodes, empty when it has none. DROP and SHOW ALIASES are [AliasCommands]'.
 */
internal class RemoteAliasCommands(
    private val engine: ExecutionEngine,
    private val dependencies: DependencyResolver,
) : PartialFunction<LogicalPlan, Planner> {
    /** Compiles a nested plan with all commands, set by the runtime that owns these commands. */
    lateinit var compile: (LogicalPlan, AdministrationCommandRuntimeContext) -> ExecutionPlan

    // Resolved on first use, like Community's runtime does.
    private val authorization by lazy {
        SecurityAuthorizationHandler(
            dependencies.resolveDependency(AbstractSecurityLog::class.java)
        )
    }
    private val config by lazy { dependencies.resolveDependency(Config::class.java) }
    private val passwords by lazy { AliasPasswords(config) }

    private val admin = `AdministrationCommandRuntime$`.`MODULE$`

    override fun isDefinedAt(plan: LogicalPlan): Boolean = planner(plan) != null

    override fun apply(plan: LogicalPlan): Planner =
        planner(plan) ?: throw IllegalArgumentException("Not a remote alias command: $plan")

    private fun planner(plan: LogicalPlan): Planner? =
        when (plan) {
            is CreateRemoteDatabaseAlias ->
                Planner { create(plan, compile(plan.source(), it), version(it)) }
            is AlterRemoteDatabaseAlias ->
                Planner { alter(plan, compile(plan.source(), it), version(it)) }
            else -> null
        }

    /**
     * Creates the remote alias, unless a database or alias has the name; with OR REPLACE, the plans
     * before dropped an alias of the same name, local or remote.
     */
    private fun create(
        plan: CreateRemoteDatabaseAlias,
        source: ExecutionPlan,
        version: CypherVersion,
    ): ExecutionPlan {
        val credentials =
            plan.remoteAliasCredentials() as? RemoteAliasStoredCredentials
                ?: return notSupportedPlan(source, "`OIDC CREDENTIAL FORWARDING`")
        val alias = admin.getDatabaseNameFields("aliasName", plan.aliasName(), false)
        val target = admin.getDatabaseNameFields("targetName", plan.targetName(), false)
        val free =
            ensureNameFree(
                engine,
                authorization,
                "database alias",
                alias.nameKey(),
                VirtualValues.map(alias.keys(), alias.values()),
                names(alias.nameConverter(), alias.nameKey(), alias.namespaceKey()),
                { show(plan.aliasName(), it) },
                source,
            )
        return updating(
            "CreateRemoteDatabaseAlias",
            """
            CREATE (alias:$DATABASE_NAME:$REMOTE {
                ${TopologyGraphDbmsModel.NAME_PROPERTY}: ${parameter(alias.nameKey())},
                ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: ${parameter(alias.namespaceKey())},
                ${TopologyGraphDbmsModel.DISPLAY_NAME_PROPERTY}: ${parameter(alias.displayNameKey())},
                ${TopologyGraphDbmsModel.PRIMARY_PROPERTY}: false,
                ${TopologyGraphDbmsModel.URL_PROPERTY}: ${parameter(URL)},
                ${TopologyGraphDbmsModel.TARGET_NAME_PROPERTY}: ${parameter(TARGET)},
                ${TopologyGraphDbmsModel.REMOTE_USERNAME_PROPERTY}: ${parameter(USER)},
                ${TopologyGraphDbmsModel.REMOTE_PASSWORD_PROPERTY}: ${parameter(PASSWORD)},
                ${TopologyGraphDbmsModel.IV_PROPERTY}: ${parameter(IV)},
                ${TopologyGraphDbmsModel.DATABASE_DEFAULT_LANGUAGE_PROPERTY}: ${parameter(LANGUAGE)},
                ${TopologyGraphDbmsModel.VERSION_PROPERTY}: randomUUID()})
            CREATE (alias)-[:$CONNECTS_WITH]->(driver:$DRIVER_SETTINGS)
            SET driver = ${parameter(DRIVER)}
            CREATE (alias)-[:$PROPERTIES]->(properties:$ALIAS_PROPERTIES)
            SET properties = ${parameter(PROPERTIES_KEY)}
            RETURN alias.${TopologyGraphDbmsModel.NAME_PROPERTY} AS name
            """
                .trimIndent(),
            VirtualValues.map(
                alias.keys() + target.keys() + LANGUAGE,
                alias.values() +
                    target.values() +
                    Values.utf8Value(
                        defaultLanguage(plan.defaultLanguageVersion(), config).persistedValue
                    ),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    val name = show(plan.aliasName(), params)
                    if (hasStatus(error, Status.Schema.ConstraintValidationFailed)) {
                        nameTaken(
                            "Failed to create the specified database alias '$name': A database " +
                                "or alias with that name already exists.",
                            name,
                            error,
                        )
                    } else {
                        failed("CREATE ALIAS", "create", name, error)
                    }
                }
            ),
            free,
            names(alias.nameConverter(), alias.nameKey(), alias.namespaceKey())
                .convert(target.nameConverter())
                .validate(
                    Function2 { transaction, params ->
                        val name = text(params, alias.displayNameKey())
                        assertValidAliasName(name)
                        val targetName = text(params, target.displayNameKey())
                        NameValidator.assertValidTargetName(targetName)
                        val operation = "create the specified database alias '$name'"
                        val driver =
                            driverSettings(
                                transaction,
                                plan.driverSettings(),
                                params,
                                operation,
                                version,
                            )
                        val url = checkedUrl(nameOf(plan.url(), params), ssl(driver))
                        val encrypted = passwords.encrypt(password(credentials.password(), params))
                        result(
                            params
                                .updatedWith(URL, Values.utf8Value(url))
                                .updatedWith(TARGET, Values.utf8Value(targetName))
                                .updatedWith(
                                    USER,
                                    Values.utf8Value(
                                        admin.runtimeStringValue(
                                            credentials.username(),
                                            params,
                                            false,
                                        )
                                    ),
                                )
                                .updatedWith(PASSWORD, Values.byteArray(encrypted.password))
                                .updatedWith(IV, Values.byteArray(encrypted.iv))
                                .updatedWith(DRIVER, driver)
                                .updatedWith(
                                    PROPERTIES_KEY,
                                    aliasProperties(
                                        transaction,
                                        plan.properties(),
                                        params,
                                        operation,
                                        version,
                                    ),
                                )
                        )
                    }
                ),
        )
    }

    /**
     * Changes what the command gives of the remote alias, replacing all driver settings or
     * properties if given; a new `version` makes connections anew whenever anything but the
     * properties changes. The plans before checked that the alias exists. A local alias stays
     * local.
     */
    private fun alter(
        plan: AlterRemoteDatabaseAlias,
        source: ExecutionPlan,
        version: CypherVersion,
    ): ExecutionPlan {
        val alias = admin.getDatabaseNameFields("aliasName", plan.aliasName(), false)
        val target =
            if (plan.targetName().isDefined) {
                admin.getDatabaseNameFields("targetName", plan.targetName().get(), false)
            } else {
                null
            }
        val aliasNames = names(alias.nameConverter(), alias.nameKey(), alias.namespaceKey())
        val node =
            "(alias:$DATABASE_NAME {${TopologyGraphDbmsModel.NAME_PROPERTY}: " +
                "${parameter(alias.nameKey())}, ${TopologyGraphDbmsModel.NAMESPACE_PROPERTY}: " +
                "${parameter(alias.namespaceKey())}, ${TopologyGraphDbmsModel.PRIMARY_PROPERTY}: " +
                "false})"
        val remote =
            updating(
                "EnsureAliasIsRemote",
                """
                MATCH $node
                OPTIONAL MATCH (alias)-[:$CONNECTS_WITH]->(driver:$DRIVER_SETTINGS)
                RETURN {remote: alias:$REMOTE, url: alias.${TopologyGraphDbmsModel.URL_PROPERTY},
                    ssl: driver.${TopologyGraphDbmsModel.SSL_ENFORCED}} AS alias
                """
                    .trimIndent(),
                VirtualValues.map(alias.keys(), alias.values()),
                QueryHandler.handleResult(
                    Function3 { _, value, params ->
                        val existing = value as MapValue
                        val name = show(plan.aliasName(), params)
                        if (!(existing.get("remote") as BooleanValue).booleanValue()) {
                            ThrowException(InvalidArgumentException.alterRemoteAliasToLocal(name))
                        } else {
                            // The URL or driver settings the alias will have must be secure.
                            if (plan.url().isDefined || plan.driverSettings().isDefined) {
                                val driver = params.get(DRIVER) as? MapValue
                                checkedUrl(
                                    if (plan.url().isDefined) {
                                        nameOf(plan.url().get(), params)
                                    } else {
                                        (existing.get("url") as TextValue).stringValue()
                                    },
                                    if (driver != null) ssl(driver)
                                    else
                                        (existing.get("ssl") as? BooleanValue)?.booleanValue()
                                            ?: true,
                                )
                            }
                            `Continue$`.`MODULE$`
                        }
                    }
                ),
                source,
                aliasNames.validate(
                    Function2 { transaction, params ->
                        result(
                            if (plan.driverSettings().isDefined) {
                                params.updatedWith(
                                    DRIVER,
                                    driverSettings(
                                        transaction,
                                        plan.driverSettings(),
                                        params,
                                        operation(plan.aliasName(), params),
                                        version,
                                    ),
                                )
                            } else {
                                params
                            }
                        )
                    }
                ),
            )
        val sets = mutableListOf<String>()
        fun set(property: String, key: String) {
            sets += "alias.$property = ${parameter(key)}"
        }
        if (target != null) set(TopologyGraphDbmsModel.TARGET_NAME_PROPERTY, TARGET)
        if (plan.url().isDefined) set(TopologyGraphDbmsModel.URL_PROPERTY, URL)
        if (plan.username().isDefined) set(TopologyGraphDbmsModel.REMOTE_USERNAME_PROPERTY, USER)
        if (plan.password().isDefined) {
            set(TopologyGraphDbmsModel.REMOTE_PASSWORD_PROPERTY, PASSWORD)
            set(TopologyGraphDbmsModel.IV_PROPERTY, IV)
        }
        if (sets.isNotEmpty() || plan.driverSettings().isDefined) {
            sets += "alias.${TopologyGraphDbmsModel.VERSION_PROPERTY} = randomUUID()"
        }
        // The connections stay as they are: only queries that start later run in it.
        val language = plan.defaultLanguage().valueOrNull()
        if (language != null) {
            set(TopologyGraphDbmsModel.DATABASE_DEFAULT_LANGUAGE_PROPERTY, LANGUAGE)
        }
        val query =
            listOfNotNull(
                    "MATCH $node",
                    sets.takeIf { it.isNotEmpty() }?.joinToString(", ", "SET "),
                    if (plan.driverSettings().isDefined) {
                        """
                        WITH alias
                        OPTIONAL MATCH (alias)-[:$CONNECTS_WITH]->(old:$DRIVER_SETTINGS)
                        DETACH DELETE old
                        WITH DISTINCT alias
                        CREATE (alias)-[:$CONNECTS_WITH]->(driver:$DRIVER_SETTINGS)
                        SET driver = ${parameter(DRIVER)}
                        """
                    } else null,
                    if (plan.properties().isDefined) {
                        """
                        WITH alias
                        MERGE (alias)-[:$PROPERTIES]->(properties:$ALIAS_PROPERTIES)
                        SET properties = ${parameter(PROPERTIES_KEY)}
                        """
                    } else null,
                    "RETURN alias.${TopologyGraphDbmsModel.NAME_PROPERTY} AS name",
                )
                .joinToString("\n")
        val names = target?.let { aliasNames.convert(it.nameConverter()) } ?: aliasNames
        return updating(
            "AlterRemoteDatabaseAlias",
            query,
            VirtualValues.map(
                alias.keys() + (target?.keys() ?: emptyArray()) + LANGUAGE,
                alias.values() +
                    (target?.values() ?: emptyArray()) +
                    (language?.let { Values.utf8Value(it.persistedValue) } ?: Values.NO_VALUE),
            ),
            QueryHandler.handleError(
                Function2 { error, params ->
                    failed("ALTER ALIAS", "alter", show(plan.aliasName(), params), error)
                }
            ),
            remote,
            names.validate(
                Function2 { transaction, params ->
                    val operation = operation(plan.aliasName(), params)
                    var updated = params
                    if (target != null) {
                        val targetName = text(params, target.displayNameKey())
                        NameValidator.assertValidTargetName(targetName)
                        updated = updated.updatedWith(TARGET, Values.utf8Value(targetName))
                    }
                    if (plan.url().isDefined) {
                        updated =
                            updated.updatedWith(
                                URL,
                                Values.utf8Value(nameOf(plan.url().get(), params)),
                            )
                    }
                    if (plan.username().isDefined) {
                        updated =
                            updated.updatedWith(
                                USER,
                                Values.utf8Value(nameOf(plan.username().get(), params)),
                            )
                    }
                    if (plan.password().isDefined) {
                        val encrypted = passwords.encrypt(password(plan.password().get(), params))
                        updated =
                            updated
                                .updatedWith(PASSWORD, Values.byteArray(encrypted.password))
                                .updatedWith(IV, Values.byteArray(encrypted.iv))
                    }
                    if (plan.driverSettings().isDefined) {
                        updated =
                            updated.updatedWith(
                                DRIVER,
                                driverSettings(
                                    transaction,
                                    plan.driverSettings(),
                                    params,
                                    operation,
                                    version,
                                ),
                            )
                    }
                    result(
                        updated.updatedWith(
                            PROPERTIES_KEY,
                            aliasProperties(
                                transaction,
                                plan.properties(),
                                params,
                                operation,
                                version,
                            ),
                        )
                    )
                }
            ),
        )
    }

    /** What failure messages of altering the alias [name] say failed. */
    private fun operation(name: DatabaseName, params: MapValue) =
        "alter the specified database alias '${show(name, params)}'"

    /**
     * The driver [settings] of an alias, evaluated and checked as Neo4j checks them: only those a
     * remote alias can have, of the right types; those not given are left out.
     */
    private fun driverSettings(
        transaction: Transaction,
        settings: Option<Either<ScalaMap<String, Expression>, Parameter>>,
        params: MapValue,
        operation: String,
        version: CypherVersion,
    ): MapValue {
        val evaluated =
            AliasMapSettingsEvaluator(
                    (transaction as InternalTransaction).kernelTransaction().procedures(),
                    version,
                )
                .convertDriverSettings(settings, params, operation)
        val builder = MapValueBuilder()
        if (evaluated.isDefined) {
            CollectionConverters.asJava(evaluated.get()).forEach { (key, value) ->
                if (value !== Values.NO_VALUE) builder.add(key, value)
            }
        }
        return builder.build()
    }

    /** Whether [driver] settings require a secure scheme: unless they say otherwise. */
    private fun ssl(driver: MapValue): Boolean =
        (driver.get(TopologyGraphDbmsModel.SSL_ENFORCED) as? BooleanValue)?.booleanValue() ?: true

    /**
     * [url], checked: a scheme of Neo4j's drivers, a secure one when [secure], and a host. Drivers
     * that route (`neo4j`) find the members of a cluster; the others (`bolt`) connect to one
     * server. `+s` trusts the certificates the JVM trusts, `+ssc` any certificate.
     */
    private fun checkedUrl(url: String, secure: Boolean): String {
        val uri =
            try {
                URI(url)
            } catch (_: URISyntaxException) {
                throw InvalidArgumentException.remoteDatabaseUrl()
            }
        val scheme = uri.scheme?.lowercase()
        if (scheme !in SCHEMES) throw InvalidArgumentException.invalidURLScheme(url, SCHEMES)
        if (secure && scheme !in SECURE_SCHEMES) {
            throw InvalidArgumentException.insecureURLScheme(url, SECURE_SCHEMES)
        }
        if (uri.host.isNullOrEmpty()) throw InvalidArgumentException.remoteDatabaseUrl()
        return url
    }

    /** The bytes of the [password], which Neo4j's planner always passes as a parameter. */
    private fun password(password: Expression, params: MapValue): ByteArray =
        admin.getValidPasswordParameter(params, (password as Parameter).name())

    private fun names(
        converter: Function2<Transaction, MapValue, MapValue>,
        nameKey: String,
        namespaceKey: String,
    ): ParameterTransformer =
        ParameterTransformer(
                ParameterTransformer.`$lessinit$greater$default$1`(),
                ParameterTransformer.`$lessinit$greater$default$2`(),
            )
            .convert(converter)
            .convert(inRoot(nameKey, namespaceKey))

    private fun result(params: MapValue) =
        Tuple2(params, `Set$`.`MODULE$`.empty<InternalNotification>())

    private fun updating(
        name: String,
        query: String,
        parameters: MapValue,
        handler: QueryHandler,
        source: ExecutionPlan,
        transformer: ParameterTransformer,
    ): ExecutionPlan =
        UpdatingSystemCommandExecutionPlan(
            name,
            engine,
            authorization,
            query,
            parameters,
            handler,
            Option.apply(source),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$8`(),
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$9`(),
            transformer,
            UpdatingSystemCommandExecutionPlan.`$lessinit$greater$default$11`(),
        )

    private fun failed(command: String, operation: String, name: String, error: Throwable) =
        when {
            hasStatus(error, Status.Cluster.NotALeader) ->
                DatabaseAdministrationOnFollowerException.notALeader(
                    command,
                    "Failed to $operation the specified database alias '$name'",
                    error,
                )
            error is Status.HasStatus -> error
            else ->
                IllegalStateException(
                    "Failed to $operation the specified database alias '$name'.",
                    error,
                )
        }

    private fun hasStatus(error: Throwable, status: Status): Boolean =
        generateSequence(error) { it.cause }.any { it is Status.HasStatus && it.status() == status }

    private fun show(name: DatabaseName, params: MapValue): String =
        admin.runtimeStringValue(name, params)

    private fun text(params: MapValue, key: String): String =
        (params.get(key) as TextValue).stringValue()

    private fun parameter(key: String) = "${'$'}`$key`"

    private companion object {
        val DATABASE_NAME = TopologyGraphDbmsModel.DATABASE_NAME_LABEL.name()
        val REMOTE = TopologyGraphDbmsModel.REMOTE_DATABASE_LABEL.name()
        val CONNECTS_WITH = TopologyGraphDbmsModel.CONNECTS_WITH_RELATIONSHIP.name()
        val DRIVER_SETTINGS = TopologyGraphDbmsModel.DRIVER_SETTINGS_LABEL.name()
        val PROPERTIES = TopologyGraphDbmsModel.PROPERTIES_RELATIONSHIP.name()
        val ALIAS_PROPERTIES = TopologyGraphDbmsModel.ALIAS_PROPERTIES_LABEL.name()

        val SCHEMES = listOf("neo4j", "neo4j+s", "neo4j+ssc", "bolt", "bolt+s", "bolt+ssc")
        val SECURE_SCHEMES = SCHEMES.filter { it.endsWith("+s") || it.endsWith("+ssc") }

        // The parameters of what a command sets, named so as not to clash with the user's.
        const val URL = "__dozendb_aliasUrl"
        const val TARGET = "__dozendb_aliasTarget"
        const val USER = "__dozendb_aliasUser"
        const val PASSWORD = "__dozendb_aliasPassword"
        const val IV = "__dozendb_aliasIv"
        const val DRIVER = "__dozendb_aliasDriver"
        const val PROPERTIES_KEY = "__dozendb_aliasProperties"
        const val LANGUAGE = "__dozendb_aliasLanguage"
    }
}
