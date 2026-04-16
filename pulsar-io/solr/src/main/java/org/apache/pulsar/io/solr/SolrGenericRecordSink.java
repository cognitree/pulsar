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
package org.apache.pulsar.io.solr;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.schema.Field;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.annotations.Connector;
import org.apache.pulsar.io.core.annotations.IOType;
import org.apache.solr.common.SolrInputDocument;

@Connector(
    name = "solr",
    type = IOType.SINK,
    help = "The SolrGenericRecordSink is used for moving messages from Pulsar to Solr.",
    configClass = SolrSinkConfig.class
)
@Slf4j
public class SolrGenericRecordSink extends SolrAbstractSink<GenericRecord> {
    @Override
    public SolrInputDocument convert(Record<GenericRecord> pulsarRecord) {
        SolrInputDocument solrDocument = new SolrInputDocument();
        GenericRecord messageValue = pulsarRecord.getValue();
        if (solrSinkConfig != null && solrSinkConfig.isUnwrapDebeziumRecord()) {
            return mapDebeziumPayload(messageValue, solrDocument);
        }

        // Default mapping for non-CDC messages
        for (Field recordField : messageValue.getFields()) {
            Object fieldValue = messageValue.getField(recordField);
            if (fieldValue != null) {
                solrDocument.setField(recordField.getName(), fieldValue);
            }
        }
        return solrDocument;
    }

    private SolrInputDocument mapDebeziumPayload(GenericRecord rootRecord, SolrInputDocument solrDocument) {
        try {
            GenericRecord payloadRecord = extractValueRecord(rootRecord);

            if (containsAfterField(payloadRecord)) {
                payloadRecord = extractAfterRecord(payloadRecord, solrDocument);
                if (payloadRecord == null) {
                    return solrDocument;
                }
            }
            populateSolrFields(payloadRecord, solrDocument);
            return solrDocument;
        } catch (Exception ex) {
            log.error("Debezium record processing failed, returning empty Solr document", ex);
            return solrDocument;
        }
    }

    private GenericRecord extractValueRecord(GenericRecord rootRecord) {
        Object nativePayload = rootRecord.getNativeObject();
        if (nativePayload instanceof KeyValue) {
            Object valuePart = ((KeyValue<?, ?>) nativePayload).getValue();
            if (valuePart instanceof GenericRecord) {
                log.debug("Detected KeyValue wrapper, extracting value section");
                return (GenericRecord) valuePart;
            }
        }
        return rootRecord;
    }

    private boolean containsAfterField(GenericRecord record) {
        for (Field field : record.getFields()) {
            if ("after".equals(field.getName())) {
                return true;
            }
        }
        return false;
    }

    private GenericRecord extractAfterRecord(GenericRecord envelopeRecord, SolrInputDocument solrDocument) {
        Object afterField = envelopeRecord.getField("after");
        if (afterField == null) {
            log.info("Debezium DELETE event detected, skipping document creation");
            return null;
        }
        if (afterField instanceof GenericRecord) {
            log.debug("Debezium envelope detected, extracting 'after' payload");
            return (GenericRecord) afterField;
        }
        return envelopeRecord;
    }

    private void populateSolrFields(GenericRecord dataRecord, SolrInputDocument solrDocument) {
        for (Field field : dataRecord.getFields()) {
            Object rawValue = dataRecord.getField(field);
            if (rawValue == null || rawValue instanceof GenericRecord) {
                continue;
            }
            solrDocument.setField(field.getName(), normalizeValue(rawValue));
        }
    }

    private Object normalizeValue(Object value) {
        if (value instanceof Integer || value instanceof Long) {
            return String.valueOf(value);
        }
        return value;
    }
}
