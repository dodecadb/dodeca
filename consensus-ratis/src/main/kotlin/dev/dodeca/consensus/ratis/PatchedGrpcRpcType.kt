package dev.dodeca.consensus.ratis

import org.apache.ratis.conf.Parameters
import org.apache.ratis.grpc.PatchedGrpcFactory
import org.apache.ratis.rpc.RpcFactory
import org.apache.ratis.rpc.RpcType

/**
 * Ratis' gRPC transport, as [PatchedGrpcFactory] changes it. Ratis creates it from its class name,
 * set as `raft.rpc.type`.
 */
internal class PatchedGrpcRpcType : RpcType {
    override fun name(): String = PatchedGrpcRpcType::class.java.name

    override fun newFactory(parameters: Parameters): RpcFactory = PatchedGrpcFactory(parameters)
}
