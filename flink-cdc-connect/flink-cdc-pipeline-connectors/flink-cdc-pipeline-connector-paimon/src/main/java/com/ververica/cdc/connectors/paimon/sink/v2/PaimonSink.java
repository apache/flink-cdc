/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.paimon.sink.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import com.ververica.cdc.common.event.Event;
import org.apache.paimon.flink.sink.FlinkStreamPartitioner;
import org.apache.paimon.flink.sink.MultiTableCommittable;
import org.apache.paimon.flink.sink.MultiTableCommittableSerializer;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.sink.CommitMessageSerializer;

import java.time.ZoneId;

/**
 * A {@link Sink} for Paimon. Maintain this package until Paimon has it own sinkV2 implementation.
 */
public class PaimonSink
        implements TwoPhaseCommittingSink<Event, MultiTableCommittable>,
                WithPreCommitTopology<Event, MultiTableCommittable> {

    private final Options catalogOptions;

    private final String commitUser;

    private final ZoneId zoneId;

    public PaimonSink(Options catalogOptions, ZoneId zoneId, String commitUser) {
        this.catalogOptions = catalogOptions;
        this.commitUser = commitUser;
        this.zoneId = zoneId;
    }

    @Override
    public PaimonWriter createWriter(InitContext context) {
        return new PaimonWriter(catalogOptions, context.metricGroup(), zoneId, commitUser);
    }

    @Override
    public Committer<MultiTableCommittable> createCommitter() {
        return new PaimonCommitter(catalogOptions, commitUser);
    }

    @Override
    public SimpleVersionedSerializer<MultiTableCommittable> getCommittableSerializer() {
        CommitMessageSerializer fileSerializer = new CommitMessageSerializer();
        return new MultiTableCommittableSerializer(fileSerializer);
    }

    @Override
    public DataStream<CommittableMessage<MultiTableCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<MultiTableCommittable>> committables) {
        TypeInformation<CommittableMessage<MultiTableCommittable>> typeInformation =
                CommittableMessageTypeInfo.of(this::getCommittableSerializer);
        // shuffle MultiTableCommittable by tables
        DataStream<CommittableMessage<MultiTableCommittable>> partitioned =
                FlinkStreamPartitioner.partition(
                        committables,
                        new MultiTableCommittableChannelComputer(),
                        committables.getParallelism());

        // add correct checkpointId to MultiTableCommittable and recreate CommittableSummary.
        return partitioned
                .transform("preCommit", typeInformation, new PreCommitOperator())
                .setParallelism(committables.getParallelism());
    }
}
