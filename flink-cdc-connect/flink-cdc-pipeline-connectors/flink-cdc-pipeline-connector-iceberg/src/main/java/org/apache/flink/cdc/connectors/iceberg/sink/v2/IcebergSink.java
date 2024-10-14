package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.CommitterInitContext;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.SupportsCommitter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.SupportsPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.iceberg.catalog.ImmutableTableCommit;

import java.io.IOException;
import java.util.Map;

public class IcebergSink
        implements Sink<Event>,
                SupportsPreWriteTopology<Event>,
                SupportsCommitter<ImmutableTableCommit> {

    // provided a default commit user.
    public static final String DEFAULT_COMMIT_USER = "admin";

    protected final Map<String, String> catalogOptions;

    protected final String commitUser;

    private final IcebergRecordSerializer<Event> serializer;

    public IcebergSink(
            Map<String, String> catalogOptions, IcebergRecordSerializer<Event> serializer) {
        this.catalogOptions = catalogOptions;
        this.serializer = serializer;
        commitUser = DEFAULT_COMMIT_USER;
    }

    public IcebergSink(
            Map<String, String> catalogOptions,
            String commitUser,
            IcebergRecordSerializer<Event> serializer) {
        this.catalogOptions = catalogOptions;
        this.commitUser = commitUser;
        this.serializer = serializer;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        return null;
    }

    @Override
    public Committer<ImmutableTableCommit> createCommitter(
            CommitterInitContext committerInitContext) throws IOException {
        return null;
    }

    @Override
    public SimpleVersionedSerializer<ImmutableTableCommit> getCommittableSerializer() {
        return null;
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext initContext) throws IOException {
        return null;
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) throws IOException {
        return Sink.super.createWriter(context);
    }
}
