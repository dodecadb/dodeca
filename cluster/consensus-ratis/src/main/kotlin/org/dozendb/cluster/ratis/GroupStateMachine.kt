package org.dozendb.cluster.ratis

import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.CompletableFuture
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto
import org.apache.ratis.protocol.ClientId
import org.apache.ratis.protocol.Message
import org.apache.ratis.protocol.RaftGroupMemberId
import org.apache.ratis.protocol.RaftPeerId
import org.apache.ratis.statemachine.TransactionContext
import org.apache.ratis.statemachine.impl.BaseStateMachine
import org.apache.ratis.thirdparty.com.google.protobuf.UnsafeByteOperations
import org.dozendb.cluster.api.DatabaseRef
import org.dozendb.cluster.api.LogEntry
import org.dozendb.cluster.api.ReplicatedStateMachine

/**
 * Adapts a [ReplicatedStateMachine] to Ratis.
 *
 * Ratis creates the adapter as soon as it finds a group, which on restart happens before the
 * database integration attaches its state machine. Applying therefore waits until [bind] is called.
 * Exceptions from [ReplicatedStateMachine.apply] propagate, which makes Ratis close the group
 * instead of skipping the entry.
 */
internal class GroupStateMachine(
    private val groupId: UUID,
    private val localClientId: ClientId,
    private val listener: Listener,
) : BaseStateMachine() {
    interface Listener {
        /** The role, leader or configuration of the group may have changed. */
        fun onGroupChanged(groupId: UUID)

        /** A write submitted by this member with [callId] is about to be appended to the log. */
        fun onAppending(callId: Long)
    }

    private val binding = CompletableFuture<ReplicatedStateMachine>()

    @Volatile
    var database: DatabaseRef = DatabaseRef("unbound", groupId)
        private set

    fun bind(database: DatabaseRef, stateMachine: ReplicatedStateMachine) {
        require(database.id == groupId) { "database $database does not belong to group $groupId" }
        if (!binding.complete(stateMachine)) {
            check(binding.getNow(null) === stateMachine) {
                "group $groupId is already bound to another state machine"
            }
        }
        this.database = database
    }

    override fun preAppendTransaction(trx: TransactionContext): TransactionContext {
        val request = trx.clientRequest
        if (request != null && request.clientId == localClientId)
            listener.onAppending(request.callId)
        return trx
    }

    override fun applyTransaction(trx: TransactionContext): CompletableFuture<Message> {
        val entry = trx.logEntry
        // get() rather than join(): Ratis interrupts this thread on shutdown.
        val target = binding.get()
        val response =
            if (entry.index > target.lastAppliedIndex()) {
                target.apply(
                    LogEntry(
                        entry.index,
                        entry.term,
                        entry.stateMachineLogEntry.logData.toByteArray(),
                    )
                )
            } else {
                ByteArray(0)
            }
        advance(entry.term, entry.index)
        return CompletableFuture.completedFuture(replyMessage(entry.term, response))
    }

    /** Ratis applied an entry that does not reach the state machine, such as a configuration. */
    override fun notifyTermIndexUpdated(term: Long, index: Long) {
        advance(term, index)
    }

    private fun advance(term: Long, index: Long) {
        val previousTerm = lastAppliedTermIndex.term
        updateLastAppliedTermIndex(term, index)
        // Applying the first entry of a term can make the leader writable.
        if (term != previousTerm) listener.onGroupChanged(groupId)
    }

    /** The term of the last entry applied by this member. */
    fun appliedTerm(): Long = lastAppliedTermIndex.term

    /** Answers read-index requests with the applied index, after Ratis confirmed leadership. */
    override fun query(request: Message): CompletableFuture<Message> =
        CompletableFuture.completedFuture(longMessage(lastAppliedTermIndex.index))

    override fun notifyLeaderChanged(groupMemberId: RaftGroupMemberId, newLeaderId: RaftPeerId?) {
        listener.onGroupChanged(groupId)
    }

    override fun notifyLeaderReady() {
        listener.onGroupChanged(groupId)
    }

    override fun notifyConfigurationChanged(
        term: Long,
        index: Long,
        newRaftConfiguration: RaftConfigurationProto,
    ) {
        listener.onGroupChanged(groupId)
    }

    override fun close() {
        super.close()
        listener.onGroupChanged(groupId)
    }

    override fun toString(): String = "GroupStateMachine($database)"

    companion object {
        fun longMessage(value: Long): Message =
            Message.valueOf(
                UnsafeByteOperations.unsafeWrap(
                    ByteBuffer.allocate(Long.SIZE_BYTES).putLong(value).flip()
                )
            )

        fun decodeLong(message: Message): Long = message.content.asReadOnlyByteBuffer().getLong()

        /** A write reply: the entry's term followed by the state machine's response. */
        fun replyMessage(term: Long, response: ByteArray): Message =
            Message.valueOf(
                UnsafeByteOperations.unsafeWrap(
                    ByteBuffer.allocate(Long.SIZE_BYTES + response.size)
                        .putLong(term)
                        .put(response)
                        .flip()
                )
            )

        /** Splits a write reply into the entry's term and the state machine's response. */
        fun decodeReply(message: Message): Pair<Long, ByteArray> {
            val buffer = message.content.asReadOnlyByteBuffer()
            val term = buffer.getLong()
            val response = ByteArray(buffer.remaining()).also { buffer.get(it) }
            return term to response
        }
    }
}
