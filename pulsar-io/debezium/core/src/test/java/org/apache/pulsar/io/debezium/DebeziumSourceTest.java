/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.io.debezium;

import static org.testng.Assert.assertEquals;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Test the config mapping logic in {@link DebeziumSource}.
 */
public class DebeziumSourceTest {

    @Test
    public void testTopicPrefixMappedToServerName() {
        // database.server.name must be auto-mapped for internal validator
        Map<String, Object> config = new HashMap<>();
        config.put("topic.prefix", "dbserver1");

        DebeziumSource.applyConfigMappings(config);

        assertEquals(config.get("database.server.name"), "dbserver1",
            "database.server.name should be auto-mapped from topic.prefix");
    }

    @Test
    public void testServerNameMappedToTopicPrefix() {
        // topic.prefix must be auto-mapped
        Map<String, Object> config = new HashMap<>();
        config.put("database.server.name", "dbserver1");

        DebeziumSource.applyConfigMappings(config);

        assertEquals(config.get("topic.prefix"), "dbserver1",
            "topic.prefix should be auto-mapped from database.server.name");
    }

    @Test
    public void testTableWhitelistMappedToIncludeList() {
        Map<String, Object> config = new HashMap<>();
        config.put("table.whitelist", "public.users");

        DebeziumSource.applyConfigMappings(config);

        assertEquals(config.get("table.include.list"), "public.users",
            "table.include.list should be auto-mapped from table.whitelist");
    }

    @Test
    public void testSchemaWhitelistMappedToIncludeList() {
        Map<String, Object> config = new HashMap<>();
        config.put("schema.whitelist", "public");

        DebeziumSource.applyConfigMappings(config);

        assertEquals(config.get("schema.include.list"), "public",
            "schema.include.list should be auto-mapped from schema.whitelist");
    }

    @Test
    public void testExistingValuesNotOverwritten() {
        Map<String, Object> config = new HashMap<>();
        config.put("topic.prefix", "prefix1");
        config.put("database.server.name", "server1");

        DebeziumSource.applyConfigMappings(config);

        assertEquals(config.get("topic.prefix"), "prefix1");
        assertEquals(config.get("database.server.name"), "server1");
    }
}
