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

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/** FlinkStreamPartitioner with {@link MultiTableCommittableChannelComputer}. */
public class FlinkStreamPartitioner<T> extends StreamPartitioner<T> {

    private final MultiTableCommittableChannelComputer channelComputer;

    public FlinkStreamPartitioner(MultiTableCommittableChannelComputer channelComputer) {
        this.channelComputer = channelComputer;
    }

    public void setup(int numberOfChannels) {
        super.setup(numberOfChannels);
        this.channelComputer.setup(numberOfChannels);
    }

    public int selectChannel(SerializationDelegate<StreamRecord<T>> record) {
        return this.channelComputer.channel(
                (CommittableMessage<WriteResultWrapper>)
                        ((StreamRecord) record.getInstance()).getValue());
    }

    public StreamPartitioner<T> copy() {
        return this;
    }

    public SubtaskStateMapper getDownstreamSubtaskStateMapper() {
        return SubtaskStateMapper.FULL;
    }

    public boolean isPointwise() {
        return false;
    }

    public String toString() {
        return this.channelComputer.toString();
    }

    public static <T> DataStream<T> partition(
            DataStream<T> input,
            MultiTableCommittableChannelComputer channelComputer,
            Integer parallelism) {
        FlinkStreamPartitioner<T> partitioner = new FlinkStreamPartitioner<>(channelComputer);
        PartitionTransformation<T> partitioned =
                new PartitionTransformation<>(input.getTransformation(), partitioner);
        if (parallelism != null) {
            partitioned.setParallelism(parallelism);
        }
        return new DataStream<>(input.getExecutionEnvironment(), partitioned);
    }
}
