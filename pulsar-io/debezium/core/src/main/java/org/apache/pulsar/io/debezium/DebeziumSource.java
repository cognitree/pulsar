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

import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.io.core.SourceContext;
import org.apache.pulsar.io.kafka.connect.KafkaConnectSource;
import org.apache.pulsar.io.kafka.connect.PulsarKafkaWorkerConfig;

@Slf4j
public abstract class DebeziumSource extends KafkaConnectSource {
    private static final String DEFAULT_CONVERTER = "org.apache.kafka.connect.json.JsonConverter";
    private static final String DEFAULT_HISTORY = "org.apache.pulsar.io.debezium.PulsarDatabaseHistory";
    private static final String DEFAULT_OFFSET_TOPIC = "debezium-offset-topic";
    private static final String DEFAULT_HISTORY_TOPIC = "debezium-history-topic";

    public static void throwExceptionIfConfigNotMatch(Map<String, Object> config,
                                                       String key,
                                                       String value) throws IllegalArgumentException {
        Object orig = config.get(key);
        if (orig == null) {
            config.put(key, value);
            return;
        }

        // throw exception if value not match
        if (!orig.equals(value)) {
            throw new IllegalArgumentException("Expected " + value + " but has " + orig);
        }
    }

    public static void setConfigIfNull(Map<String, Object> config, String key, String value) {
        config.putIfAbsent(key, value);
    }

    // namespace for output topics, default value is "tenant/namespace"
    public static String topicNamespace(SourceContext sourceContext) {
        String tenant = sourceContext.getTenant();
        String namespace = sourceContext.getNamespace();

        return (StringUtils.isEmpty(tenant) ? TopicName.PUBLIC_TENANT : tenant) + "/"
                + (StringUtils.isEmpty(namespace) ? TopicName.DEFAULT_NAMESPACE : namespace);
    }

    public static void tryLoadingConfigSecret(String secretName, Map<String, Object> config, SourceContext context) {
        try {
            String secret = context.getSecret(secretName);
            if (secret != null) {
                config.put(secretName, secret);
                log.info("Config key {} set from secret.", secretName);
            }
        } catch (Exception e) {
            log.warn("Failed to read secret {}.", secretName, e);
        }
    }

    public abstract void setDbConnectorTask(Map<String, Object> config) throws Exception;

    public static void applyConfigMappings(Map<String, Object> config) {
        // Translate database.server.name to topic.prefix
        if (config.containsKey("database.server.name") && !config.containsKey("topic.prefix")) {
            config.put("topic.prefix", config.get("database.server.name"));
            log.warn("Property 'database.server.name' is deprecated in Debezium 2.x. Auto-mapped to 'topic.prefix'");
        }

        // Translate topic.prefix back to database.server.name
        if (config.containsKey("topic.prefix") && !config.containsKey("database.server.name")) {
            config.put("database.server.name", config.get("topic.prefix"));
            log.warn("Auto-mapped 'topic.prefix' to 'database.server.name' for internal validator compatibility");
        }

        // Translate table.whitelist to table.include.list
        if (config.containsKey("table.whitelist") && !config.containsKey("table.include.list")) {
            config.put("table.include.list", config.get("table.whitelist"));
            log.warn("Property 'table.whitelist' is deprecated in Debezium 2.x. Auto-mapped to 'table.include.list'");
        }

        // Translate schema.whitelist to schema.include.list
        if (config.containsKey("schema.whitelist") && !config.containsKey("schema.include.list")) {
            config.put("schema.include.list", config.get("schema.whitelist"));
            log.warn("Property 'schema.whitelist' is deprecated in Debezium 2.x. Auto-mapped to 'schema.include.list'");
        }
    }

    @Override
    public void open(Map<String, Object> incomingConfig, SourceContext sourceContext) throws Exception {
        Map<String, Object> config = new HashMap<>(incomingConfig);
        setDbConnectorTask(config);
        tryLoadingConfigSecret("database.user", config, sourceContext);
        tryLoadingConfigSecret("database.password", config, sourceContext);

        // Apply backward compatibility mappings
        applyConfigMappings(config);
        // key.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.KEY_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);
        // value.converter
        setConfigIfNull(config, PulsarKafkaWorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, DEFAULT_CONVERTER);

        // database.history : implementation class for database history.
        setConfigIfNull(config, HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY.name(), DEFAULT_HISTORY);

        // database.history.pulsar.service.url
        String pulsarUrl = (String) config.get(PulsarDatabaseHistory.SERVICE_URL.name());

        String topicNamespace = topicNamespace(sourceContext);
        // topic.namespace
        setConfigIfNull(config, PulsarKafkaWorkerConfig.TOPIC_NAMESPACE_CONFIG, topicNamespace);

        String sourceName = sourceContext.getSourceName();
        // database.history.pulsar.topic: history topic name
        setConfigIfNull(config, PulsarDatabaseHistory.TOPIC.name(),
            topicNamespace + "/" + sourceName + "-" + DEFAULT_HISTORY_TOPIC);
        // offset.storage.topic: offset topic name
        setConfigIfNull(config, PulsarKafkaWorkerConfig.OFFSET_STORAGE_TOPIC_CONFIG,
            topicNamespace + "/" + sourceName + "-" + DEFAULT_OFFSET_TOPIC);

        // pass pulsar.client.builder if database.history.pulsar.service.url is not provided
        if (StringUtils.isEmpty(pulsarUrl)) {
            String pulsarClientBuilder = SerDeUtils.serialize(sourceContext.getPulsarClientBuilder());
            config.put(PulsarDatabaseHistory.CLIENT_BUILDER.name(), pulsarClientBuilder);
        }

        super.open(config, sourceContext);
    }

}
