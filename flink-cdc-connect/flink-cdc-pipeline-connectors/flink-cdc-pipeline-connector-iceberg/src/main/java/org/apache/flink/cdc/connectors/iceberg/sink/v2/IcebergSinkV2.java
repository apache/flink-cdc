package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import java.io.IOException;
import java.util.Map;

public class IcebergSinkV2<InputT> implements WithPreCommitTopology<InputT, IcebergCommittable> {

    // provided a default commit user.
    public static final String DEFAULT_COMMIT_USER = "admin";

    protected final Map<String, String> catalogOptions;

    protected final String commitUser;

    private final IcebergRecordSerializer<Event> serializer;

    public IcebergSinkV2(
            Map<String, String> catalogOptions, IcebergRecordSerializer<Event> serializer) {
        this.catalogOptions = catalogOptions;
        this.serializer = serializer;
        commitUser = DEFAULT_COMMIT_USER;
    }

    public IcebergSinkV2(
            Map<String, String> catalogOptions,
            String commitUser,
            IcebergRecordSerializer<Event> serializer) {
        this.catalogOptions = catalogOptions;
        this.commitUser = commitUser;
        this.serializer = serializer;
    }

    @Override
    public Committer<IcebergCommittable> createCommitter() {
        return new IcebergCommitter(catalogOptions, commitUser);
    }

    @Override
    public SimpleVersionedSerializer<IcebergCommittable> getCommittableSerializer() {
        return new IcebergCommittableSerializer();
    }

    @Override
    public SinkWriter<InputT> createWriter(InitContext initContext) throws IOException {
        return null;
    }

    @Override
    public DataStream<CommittableMessage<IcebergCommittable>> addPreCommitTopology(
            DataStream<CommittableMessage<IcebergCommittable>> dataStream) {
        return dataStream;
    }
}
