package org.apache.ratis.grpc;

import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.grpc.server.PatchedGrpcLogAppender;
import org.apache.ratis.grpc.server.PatchedGrpcServicesImpl;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.thirdparty.io.grpc.ServerCredentials;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;

/**
 * Dodeca: Ratis' gRPC transport, with {@link PatchedGrpcLogAppender} as the leaders' log appender and
 * {@link PatchedGrpcServicesImpl} as the members' services. It takes Ratis' {@link GrpcFactory} as is
 * otherwise; it is in its package for the package-private helpers that build the TLS contexts of the
 * services, as {@link GrpcFactory#newRaftServerRpc} does.
 */
public class PatchedGrpcFactory extends GrpcFactory {
  private final GrpcServices.Customizer servicesCustomizer;
  private final ServerCredentials serverCredentials;
  private final GrpcTlsConfig tlsConfig;
  private final GrpcTlsConfig adminTlsConfig;
  private final GrpcTlsConfig clientTlsConfig;
  private final GrpcTlsConfig serverTlsConfig;

  public PatchedGrpcFactory(Parameters parameters) {
    super(parameters);
    this.servicesCustomizer = GrpcConfigKeys.Server.servicesCustomizer(parameters);
    this.serverCredentials = GrpcConfigKeys.Server.credentials(parameters);
    this.tlsConfig = GrpcConfigKeys.TLS.conf(parameters);
    this.adminTlsConfig = GrpcConfigKeys.Admin.tlsConf(parameters);
    this.clientTlsConfig = GrpcConfigKeys.Client.tlsConf(parameters);
    this.serverTlsConfig = GrpcConfigKeys.Server.tlsConf(parameters);
  }

  @Override
  public LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo f) {
    return new PatchedGrpcLogAppender(server, state, f);
  }

  @Override
  public GrpcServices newRaftServerRpc(RaftServer server) {
    checkPooledByteBufAllocatorUseCacheForAllThreads(LOG::info);

    final SslContext forServer = BUILD_SSL_CONTEXT_FOR_SERVER.apply(tlsConfig, null);
    final SslContext forClient = BUILD_SSL_CONTEXT_FOR_CLIENT.apply(tlsConfig, null);
    return PatchedGrpcServicesImpl.newBuilder()
        .setServer(server)
        .setCustomizer(servicesCustomizer)
        .setServerCredentials(serverCredentials)
        .setAdminSslContext(BUILD_SSL_CONTEXT_FOR_SERVER.apply(adminTlsConfig, forServer))
        .setServerSslContextForServer(BUILD_SSL_CONTEXT_FOR_SERVER.apply(serverTlsConfig, forServer))
        .setServerSslContextForClient(BUILD_SSL_CONTEXT_FOR_CLIENT.apply(serverTlsConfig, forClient))
        .setClientSslContext(BUILD_SSL_CONTEXT_FOR_SERVER.apply(clientTlsConfig, forServer))
        .build();
  }
}
