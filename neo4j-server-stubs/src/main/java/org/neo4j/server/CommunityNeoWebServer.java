/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * Modifications Copyright (c) DozenDB
 * 2026-11-09: only the signatures DozenDB compiles against, of Neo4j Community's class of the same
 * name in neo4j-server, which Neo4j does not publish to Maven Central. Never packaged: at runtime
 * the class in Neo4j's lib/ is used.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 */
package org.neo4j.server;

import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.collection.Dependencies;
import org.neo4j.configuration.Config;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.Monitors;
import org.neo4j.server.modules.ServerModule;
import org.neo4j.server.web.WebServer;
import org.neo4j.time.SystemNanoClock;

public class CommunityNeoWebServer extends AbstractNeoWebServer {
    public CommunityNeoWebServer(
            DatabaseManagementService managementService,
            TransactionManager transactionManager,
            Dependencies globalDependencies,
            Config config,
            InternalLogProvider userLogProvider,
            DbmsInfo dbmsInfo,
            MemoryPools memoryPools,
            Monitors monitors,
            SystemNanoClock clock) {
        super(
                managementService,
                globalDependencies,
                config,
                userLogProvider,
                dbmsInfo,
                memoryPools,
                transactionManager,
                monitors,
                clock);
    }

    @Override
    protected Iterable<ServerModule> createServerModules() {
        throw new UnsupportedOperationException("a stub");
    }

    @Override
    protected WebServer createWebServer() {
        throw new UnsupportedOperationException("a stub");
    }
}
