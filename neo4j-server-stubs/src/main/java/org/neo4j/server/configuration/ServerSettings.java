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
package org.neo4j.server.configuration;

import java.util.Set;
import org.neo4j.graphdb.config.Setting;

public class ServerSettings {
    public static final Setting<Set<ConfigurableServerModules>> http_enabled_modules = null;
}
