package org.dozendb.consensus.ratis

import org.apache.ratis.conf.Parameters
import org.apache.ratis.grpc.GrpcFactory
import org.apache.ratis.grpc.server.PatchedGrpcLogAppender
import org.apache.ratis.rpc.RpcFactory
import org.apache.ratis.rpc.RpcType
import org.apache.ratis.server.RaftServer
import org.apache.ratis.server.leader.FollowerInfo
import org.apache.ratis.server.leader.LeaderState
import org.apache.ratis.server.leader.LogAppender

/**
 * Ratis' gRPC transport, with [PatchedGrpcLogAppender] as the leaders' log appender. Ratis creates
 * it from its class name, set as `raft.rpc.type`.
 */
internal class PatchedGrpcRpcType : RpcType {
    override fun name(): String = PatchedGrpcRpcType::class.java.name

    override fun newFactory(parameters: Parameters): RpcFactory = Factory(parameters)

    private class Factory(parameters: Parameters) : GrpcFactory(parameters) {
        override fun newLogAppender(
            server: RaftServer.Division,
            state: LeaderState,
            follower: FollowerInfo,
        ): LogAppender = PatchedGrpcLogAppender(server, state, follower)
    }
}
