/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/*
 * Dodeca: a copy of GrpcServerProtocolClient from Apache Ratis 3.3.1, changed on 2026-11-15 where a
 * comment starts with "Dodeca". It is formatted as upstream, so that it diffs against the upstream
 * file.
 */
package org.apache.ratis.grpc.server;

import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.util.NettyUtils;
import org.apache.ratis.grpc.util.StreamObserverWithTimeout;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NegotiationType;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.CallStreamObserver;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.apache.ratis.thirdparty.io.netty.channel.ChannelOption;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceBlockingStub;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc.RaftServerProtocolServiceStub;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.netty.channel.EventLoopGroup;
import org.apache.ratis.thirdparty.io.netty.handler.ssl.SslContext;
import org.apache.ratis.util.TimeDuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * This is a RaftClient implementation that supports streaming data to the raft
 * ring. The stream implementation utilizes gRPC.
 *
 * Dodeca: Ratis' connections to another member did not notice when the member stopped answering, as
 * when the network between them drops packets. A connection then stays open as long as TCP keeps
 * resending what the member did not acknowledge, up to a quarter of an hour, waiting twice as long
 * before each resend, up to two minutes: once the network healed, nothing reached the member until
 * the next resend, as long after as the outage had lasted. The leader's appends kept failing, and the
 * member stayed behind, unable to count towards a majority. Connections ping the member now, and
 * close when it stops answering; Ratis opens a new one with its next request, and tries again at
 * once each time it fails.
 *
 * Dodeca: public for its keep-alive times, which the members' servers permit and use too.
 */
public class PatchedGrpcServerProtocolClient implements Closeable {
  /**
   * Dodeca: a connection that received nothing for this long pings the member; it is also gRPC's
   * shortest keep-alive time.
   */
  public static final long KEEP_ALIVE_SECONDS = 10;
  /**
   * Dodeca: a connection closes when a ping, or any data it sent, goes unacknowledged for this long.
   * On Linux, gRPC sets it as the connection's TCP_USER_TIMEOUT, so a connection that stops getting
   * through closes this long after its first unacknowledged heartbeat. It must exceed the time data
   * spends queued on a busy link, as behind a database copy.
   */
  public static final long KEEP_ALIVE_TIMEOUT_SECONDS = 10;
  /**
   * Dodeca: an attempt to connect fails after this long, so that while the member cannot be reached,
   * a new attempt starts every few seconds, rather than one waiting out the growing pauses of TCP
   * between its connection requests, for Netty's 30 seconds.
   */
  private static final int CONNECT_TIMEOUT_MILLIS = 3_000;

  // Common channel
  private final ManagedChannel channel;
  private final GrpcStubPool<RaftServerProtocolServiceStub> pool;
  // Channel and stub for heartbeat
  private ManagedChannel hbChannel;
  private RaftServerProtocolServiceStub hbAsyncStub;
  private final RaftServerProtocolServiceStub asyncStub;
  private final RaftServerProtocolServiceBlockingStub blockingStub;
  private final boolean useSeparateHBChannel;

  private final TimeDuration requestTimeoutDuration;
  private static final Logger LOG = LoggerFactory.getLogger(GrpcServerProtocolClient.class);
  //visible for using in log / error messages AND to use in instrumented tests
  private final RaftPeerId raftPeerId;
  private final EventLoopGroup eventLoopGroup;

  PatchedGrpcServerProtocolClient(RaftPeer target, int connections, int flowControlWindow,
      TimeDuration requestTimeout, SslContext sslContext, boolean separateHBChannel,
      EventLoopGroup eventLoopGroup) {
    raftPeerId = target.getId();
    LOG.info("Build channel for {}", target);
    useSeparateHBChannel = separateHBChannel;
    this.eventLoopGroup = eventLoopGroup;
    channel = buildChannel(target, flowControlWindow, sslContext);
    blockingStub = RaftServerProtocolServiceGrpc.newBlockingStub(channel);
    asyncStub = RaftServerProtocolServiceGrpc.newStub(channel);
    if (useSeparateHBChannel) {
      hbChannel = buildChannel(target, flowControlWindow, sslContext);
      hbAsyncStub = RaftServerProtocolServiceGrpc.newStub(hbChannel);
    }
    requestTimeoutDuration = requestTimeout;
    this.pool = connections == 1? null : newGrpcStubPool(target.getAddress(), sslContext, connections);
  }

  // Dodeca: unused, as raft.grpc.server.stub.pool.size stays 1; the pool's connections would not ping.
  GrpcStubPool<RaftServerProtocolServiceStub> newGrpcStubPool(String address, SslContext sslContext, int connections) {
    return new GrpcStubPool<>(connections, address, sslContext, RaftServerProtocolServiceGrpc::newStub, 16,
        eventLoopGroup);
  }

  private ManagedChannel buildChannel(RaftPeer target, int flowControlWindow, SslContext sslContext) {
    NettyChannelBuilder channelBuilder =
        NettyChannelBuilder.forTarget(target.getAddress());
    // ignore any http proxy for grpc
    channelBuilder.proxyDetector(uri -> null);

    if (sslContext != null) {
      channelBuilder.useTransportSecurity().sslContext(sslContext);
    } else {
      channelBuilder.negotiationType(NegotiationType.PLAINTEXT);
    }
    channelBuilder.disableRetry();
    // Dodeca: notices when the member stops answering; see the class comment.
    channelBuilder.keepAliveTime(KEEP_ALIVE_SECONDS, TimeUnit.SECONDS)
        .keepAliveTimeout(KEEP_ALIVE_TIMEOUT_SECONDS, TimeUnit.SECONDS)
        .keepAliveWithoutCalls(true)
        .withOption(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MILLIS);
    if (eventLoopGroup != null) {
      channelBuilder.channelType(NettyUtils.getSocketChannelClass(eventLoopGroup))
          .eventLoopGroup(eventLoopGroup);
    }
    return channelBuilder.flowControlWindow(flowControlWindow).build();
  }

  @Override
  public void close() {
    LOG.info("{} Close channels", raftPeerId);
    if (useSeparateHBChannel) {
      GrpcUtil.shutdownManagedChannel(hbChannel);
    }
    GrpcUtil.shutdownManagedChannel(channel);
    if (pool != null) {
      pool.close();
    }
  }

  public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) {
    // the StatusRuntimeException will be handled by the caller
    RequestVoteReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .requestVote(request);
    return r;
  }

  public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) {
    StartLeaderElectionReplyProto r =
        blockingStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
            .startLeaderElection(request);
    return r;
  }

  void readIndex(ReadIndexRequestProto request, StreamObserver<ReadIndexReplyProto> s) {
    if (pool == null) {
      asyncStub.withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
          .readIndex(request, s);
    } else {
      GrpcStubPool.Stub<RaftServerProtocolServiceStub> p;
      try {
        p = pool.acquire();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        s.onError(e);
        return;
      }
      p.getStub().withDeadlineAfter(requestTimeoutDuration.getDuration(), requestTimeoutDuration.getUnit())
          .readIndex(request, new StreamObserver<ReadIndexReplyProto>() {
            @Override
            public void onNext(ReadIndexReplyProto v) {
              s.onNext(v);
            }

            @Override
            public void onError(Throwable t) {
              try {
                s.onError(t);
              } finally {
                p.release();
              }
            }

            @Override
            public void onCompleted() {
              try {
                s.onCompleted();
              } finally {
                p.release();
              }
            }
          });
    }
  }

  CallStreamObserver<AppendEntriesRequestProto> appendEntries(
      StreamObserver<AppendEntriesReplyProto> responseHandler, boolean isHeartbeat) {
    if (isHeartbeat && useSeparateHBChannel) {
      return (CallStreamObserver<AppendEntriesRequestProto>) hbAsyncStub.appendEntries(responseHandler);
    } else {
      return (CallStreamObserver<AppendEntriesRequestProto>) asyncStub.appendEntries(responseHandler);
    }
  }

  StreamObserver<InstallSnapshotRequestProto> installSnapshot(
      String name, TimeDuration timeout, int limit, StreamObserver<InstallSnapshotReplyProto> responseHandler) {
    return StreamObserverWithTimeout.newInstance(name, ServerStringUtils::toInstallSnapshotRequestString,
        () -> timeout, limit, i -> asyncStub.withInterceptors(i).installSnapshot(responseHandler));
  }

  // short-circuit the backoff timer and make them reconnect immediately.
  public void resetConnectBackoff() {
    if (useSeparateHBChannel) {
      hbChannel.resetConnectBackoff();
    }
    channel.resetConnectBackoff();
  }
}
