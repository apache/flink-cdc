package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.runtime.io.network.api.writer.SubtaskStateMapper;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

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
