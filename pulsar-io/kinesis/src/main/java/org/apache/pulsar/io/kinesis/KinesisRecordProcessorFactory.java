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
package org.apache.pulsar.io.kinesis;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.pulsar.io.core.SourceContext;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.processor.ShardRecordProcessorFactory;

public class KinesisRecordProcessorFactory implements ShardRecordProcessorFactory {
    private final LinkedBlockingQueue<KinesisRecord> queue;
    private final KinesisSourceConfig kinesisSourceConfig;
    private final SourceContext sourceContext;
    private final ScheduledExecutorService checkpointExecutor;

    public KinesisRecordProcessorFactory(LinkedBlockingQueue<KinesisRecord> queue,
                                         KinesisSourceConfig kinesisSourceConfig,
                                         SourceContext sourceContext,
                                         ScheduledExecutorService checkpointExecutor) {
        this.queue = queue;
        this.kinesisSourceConfig = kinesisSourceConfig;
        this.sourceContext = sourceContext;
        this.checkpointExecutor = checkpointExecutor;
    }

    @Override
    public ShardRecordProcessor shardRecordProcessor() {
        return new KinesisRecordProcessor(queue, kinesisSourceConfig, sourceContext, checkpointExecutor);
    }
}