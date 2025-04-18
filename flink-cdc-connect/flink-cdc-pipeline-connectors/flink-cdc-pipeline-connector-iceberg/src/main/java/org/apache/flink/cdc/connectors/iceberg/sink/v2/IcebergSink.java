/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOperator;
import org.apache.flink.cdc.connectors.iceberg.sink.v2.compaction.CompactionOptions;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.SinkCommitterMetricGroup;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.time.ZoneId;
import java.util.Map;
import java.util.Objects;

/** A {@link Sink} implementation for Apache Iceberg. */
public class IcebergSink
        implements Sink<Event>,
                WithPreWriteTopology<Event>,
                WithPreCommitTopology<Event, WriteResultWrapper>,
                TwoPhaseCommittingSink<Event, WriteResultWrapper>,
                WithPostCommitTopology<Event, WriteResultWrapper> {

    protected final Map<String, String> catalogOptions;
    protected final Map<String, String> tableOptions;

    private final ZoneId zoneId;

    private final CompactionOptions compactionOptions;

    public IcebergSink(
            Map<String, String> catalogOptions,
            Map<String, String> tableOptions,
            ZoneId zoneId,
            CompactionOptions compactionOptions) {
        this.catalogOptions = catalogOptions;
        this.tableOptions = tableOptions;
        this.zoneId = zoneId;
        this.compactionOptions = compactionOptions;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        return dataStream;
    }

    @Override
    public Committer<WriteResultWrapper> createCommitter() {
        return new IcebergCommitter(catalogOptions);
    }

    @Override
    public Committer<WriteResultWrapper> createCommitter(
            CommitterInitContext committerInitContext) {
        SinkCommitterMetricGroup metricGroup = committerInitContext.metricGroup();
        return new IcebergCommitter(catalogOptions, metricGroup);
    }

    @Override
    public SimpleVersionedSerializer<WriteResultWrapper> getCommittableSerializer() {
        return new WriteResultWrapperSerializer();
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) {
        return new IcebergWriter(
                catalogOptions,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber(),
                zoneId);
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) {
        return new IcebergWriter(
                catalogOptions,
                context.getTaskInfo().getIndexOfThisSubtask(),
                context.getTaskInfo().getAttemptNumber(),
                zoneId);
    }

    @Override
    public DataStream<CommittableMessage<WriteResultWrapper>> addPreCommitTopology(
            DataStream<CommittableMessage<WriteResultWrapper>> committables) {
        // Refer to
        // https://github.com/apache/iceberg/blob/1d9fefeb9680d782dc128f242604903e71c32f97/flink/v1.19/flink/src/main/java/org/apache/iceberg/flink/sink/IcebergSink.java#L106-L119.
        return committables.global();
    }

    @Override
    public SimpleVersionedSerializer<WriteResultWrapper> getWriteResultSerializer() {
        return new WriteResultWrapperSerializer();
    }

    @Override
    public void addPostCommitTopology(
            DataStream<CommittableMessage<WriteResultWrapper>> committableMessageDataStream) {
        if (compactionOptions.isEnabled()) {
            TypeInformation<CommittableMessage<WriteResultWrapper>> typeInformation =
                    CommittableMessageTypeInfo.of(this::getCommittableSerializer);

            int parallelism =
                    compactionOptions.getParallelism() == -1
                            ? committableMessageDataStream.getParallelism()
                            : compactionOptions.getParallelism();

            // Shuffle by different table id.
            DataStream<CommittableMessage<WriteResultWrapper>> keyedStream =
                    committableMessageDataStream.partitionCustom(
                            (bucket, numPartitions) -> bucket % numPartitions,
                            (committableMessage) -> {
                                if (committableMessage instanceof CommittableWithLineage) {
                                    WriteResultWrapper multiTableCommittable =
                                            ((CommittableWithLineage<WriteResultWrapper>)
                                                            committableMessage)
                                                    .getCommittable();
                                    TableId tableId = multiTableCommittable.getTableId();
                                    return tableId.hashCode();
                                } else {
                                    return Objects.hash(committableMessage);
                                }
                            });

            // Small file compaction.
            keyedStream
                    .transform(
                            "Compaction",
                            typeInformation,
                            new CompactionOperator(catalogOptions, compactionOptions))
                    .setParallelism(parallelism);
        }
    }
}
