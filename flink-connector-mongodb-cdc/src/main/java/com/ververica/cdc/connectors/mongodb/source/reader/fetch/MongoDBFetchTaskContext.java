/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.source.reader.fetch;

import com.mongodb.client.model.changestream.OperationType;
import com.ververica.cdc.connectors.base.source.meta.offset.Offset;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.reader.external.FetchTask;
import com.ververica.cdc.connectors.mongodb.internal.MongoDBEnvelope;
import com.ververica.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import com.ververica.cdc.connectors.mongodb.source.dialect.MongoDBDialect;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamDescriptor;
import com.ververica.cdc.connectors.mongodb.source.offset.ChangeStreamOffset;
import com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.util.LoggingContext;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.ververica.cdc.connectors.mongodb.source.utils.BsonUtils.compareBsonValue;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.getDocumentKey;
import static com.ververica.cdc.connectors.mongodb.source.utils.MongoRecordUtils.getResumeToken;

/** The context for fetch task that fetching data of snapshot split from MongoDB data source. */
public class MongoDBFetchTaskContext implements FetchTask.Context {

    private final MongoDBDialect dialect;
    private final MongoDBSourceConfig sourceConfig;
    private final ChangeStreamDescriptor changeStreamDescriptor;
    private ChangeEventQueue<DataChangeEvent> changeEventQueue;

    public MongoDBFetchTaskContext(
            MongoDBDialect dialect,
            MongoDBSourceConfig sourceConfig,
            ChangeStreamDescriptor changeStreamDescriptor) {
        this.dialect = dialect;
        this.sourceConfig = sourceConfig;
        this.changeStreamDescriptor = changeStreamDescriptor;
    }

    public void configure(SourceSplitBase sourceSplitBase) {
        final int queueSize =
                sourceSplitBase.isSnapshotSplit() ? Integer.MAX_VALUE : sourceConfig.getBatchSize();

        this.changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(sourceConfig.getPollAwaitTimeMillis()))
                        .maxBatchSize(sourceConfig.getPollMaxBatchSize())
                        .maxQueueSize(queueSize)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "mongodb-cdc",
                                                "mongodb-cdc-connector",
                                                "mongodb-cdc-connector-task"))
                        // do not buffer any element, we use signal event
                        // .buffering()
                        .build();
    }

    public MongoDBSourceConfig getSourceConfig() {
        return sourceConfig;
    }

    public MongoDBDialect getDialect() {
        return dialect;
    }

    public ChangeStreamDescriptor getChangeStreamDescriptor() {
        return changeStreamDescriptor;
    }

    public ChangeEventQueue<DataChangeEvent> getQueue() {
        return changeEventQueue;
    }

    @Override
    public TableId getTableId(SourceRecord record) {
        return MongoRecordUtils.getTableId(record);
    }

    @Override
    public Tables.TableFilter getTableFilter() {
        // We have pushed down the filters to server side.
        return Tables.TableFilter.includeAll();
    }

    @Override
    public Offset getStreamOffset(SourceRecord record) {
        return new ChangeStreamOffset(getResumeToken(record));
    }

    @Override
    public boolean isDataChangeRecord(SourceRecord record) {
        return MongoRecordUtils.isDataChangeRecord(record);
    }

    @Override
    public boolean isRecordBetween(SourceRecord record, Object[] splitStart, Object[] splitEnd) {
        BsonDocument documentKey = getDocumentKey(record);
        // In the case of a compound index, we can also agree to only compare the range of the first
        // key.
        BsonDocument splitKeys = (BsonDocument) splitStart[0];
        String firstKey = splitKeys.getFirstKey();
        BsonValue keyValue = documentKey.get(firstKey);
        BsonValue lowerBound = ((BsonDocument) splitEnd[1]).get(firstKey);
        BsonValue upperBound = ((BsonDocument) splitEnd[1]).get(firstKey);

        // for all range
        if (lowerBound.getBsonType() == BsonType.MIN_KEY
                && upperBound.getBsonType() == BsonType.MAX_KEY) {
            return true;
        }

        // keyValue -> [lower, upper)
        if (compareBsonValue(lowerBound, keyValue) <= 0
                && compareBsonValue(keyValue, upperBound) < 0) {
            return true;
        }

        return false;
    }

    @Override
    public void rewriteOutputBuffer(
            Map<Struct, SourceRecord> outputBuffer, SourceRecord changeRecord) {
        Struct key = (Struct) changeRecord.key();
        Struct value = (Struct) changeRecord.value();
        if (value != null) {
            OperationType operation =
                    OperationType.fromString(value.getString(MongoDBEnvelope.OPERATION_TYPE_FIELD));
            switch (operation) {
                case INSERT:
                case UPDATE:
                case REPLACE:
                    outputBuffer.put(key, changeRecord);
                    break;
                case DELETE:
                    outputBuffer.remove(key);
                    break;
                default:
                    throw new IllegalStateException(
                            String.format(
                                    "Data change record meet UNKNOWN operation, the the record is %s.",
                                    changeRecord));
            }
        }
    }

    @Override
    public List<SourceRecord> formatMessageTimestamp(Collection<SourceRecord> snapshotRecords) {
        return snapshotRecords.stream()
                .peek(
                        record -> {
                            Struct value = (Struct) record.value();
                            // set message timestamp (source.ts_ms) to 0L
                            Struct source =
                                    new Struct(
                                            value.schema()
                                                    .field(MongoDBEnvelope.SOURCE_FIELD)
                                                    .schema());
                            source.put(MongoDBEnvelope.TIMESTAMP_KEY_FIELD, 0L);
                            source.put(MongoDBEnvelope.SNAPSHOT_KEY_FIELD, "true");
                            value.put(MongoDBEnvelope.SOURCE_FIELD, source);
                        })
                .collect(Collectors.toList());
    }
}
