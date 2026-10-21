package org.dozendb.backup.cli

import org.neo4j.cli.CommandProvider
import org.neo4j.cli.CommandType
import org.neo4j.cli.ExecutionContext

// Registered in META-INF/services. Community has no provider of these command types; the priority
// still differs from the default, as two providers of a type with the same one break neo4j-admin.
private const val PRIORITY = Int.MAX_VALUE - 1

class BackupCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = BackupCommand(ctx)

    override fun commandType() = CommandType.BACKUP

    override fun getPriority() = PRIORITY
}

class RestoreCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = RestoreCommand(ctx)

    override fun commandType() = CommandType.RESTORE

    override fun getPriority() = PRIORITY
}

class InspectBackupCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = InspectBackupCommand(ctx)

    override fun commandType() = CommandType.INSPECT_BACKUP

    override fun getPriority() = PRIORITY
}

class AggregateBackupCommandProvider : CommandProvider {
    override fun createCommand(ctx: ExecutionContext) = AggregateBackupCommand(ctx)

    override fun commandType() = CommandType.AGGREGATE_NEXT

    override fun getPriority() = PRIORITY
}
