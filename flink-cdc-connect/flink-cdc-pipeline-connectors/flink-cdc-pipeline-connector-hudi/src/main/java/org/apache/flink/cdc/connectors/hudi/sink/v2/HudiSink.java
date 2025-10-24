package org.apache.flink.cdc.connectors.hudi.sink.v2;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.hudi.sink.bucket.BucketAssignOperator;
import org.apache.flink.cdc.connectors.hudi.sink.bucket.BucketWrapper;
import org.apache.flink.cdc.connectors.hudi.sink.bucket.FlushEventAlignmentOperator;
import org.apache.flink.cdc.connectors.hudi.sink.operator.MultiTableWriteOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.ZoneId;

public class HudiSink implements Sink<Event>, WithPreWriteTopology<Event> {

    private static final Logger LOG = LoggerFactory.getLogger(HudiSink.class);

    private final Configuration conf;
    // TODO: Check if these are used, remove them if unused
    private final RowType rowType;
    private final boolean overwrite;
    private final boolean isBounded;

    private final String schemaOperatorUid;

    public HudiSink(
            Configuration conf,
            RowType rowType,
            boolean overwrite,
            boolean isBounded,
            String schemaOperatorUid,
            ZoneId zoneId) {
        LOG.info("Creating Hoodie sink with conf: {}", conf);
        this.conf = conf;
        this.rowType = rowType;
        this.overwrite = overwrite;
        this.isBounded = isBounded;
        this.schemaOperatorUid = schemaOperatorUid;
    }

    @Override
    public SinkWriter<Event> createWriter(InitContext context) throws IOException {
        return DummySinkWriter.INSTANCE;
    }

    @Override
    public SinkWriter<Event> createWriter(WriterInitContext context) throws IOException {
        return DummySinkWriter.INSTANCE;
    }

    @Override
    public DataStream<Event> addPreWriteTopology(DataStream<Event> dataStream) {
        LOG.info("Building Hudi pre-write topology with bucket assignment and partitioning");

        // Step 1: Bucket assignment operator
        // - Calculates bucket for DataChangeEvents
        // - Broadcasts schema events to all tasks
        // - Wraps events in BucketWrapper for downstream partitioning
        DataStream<BucketWrapper> bucketAssignedStream =
                dataStream
                        .transform(
                                "bucket_assign",
                                TypeInformation.of(BucketWrapper.class),
                                new BucketAssignOperator(conf, schemaOperatorUid))
                        .uid("bucket_assign");

        // Step 2: Partition by bucket index
        // - Routes events to tasks based on bucket index
        // - Schema events are broadcast (sent to all bucket indices)
        // - Data events go to their specific bucket's task
        DataStream<BucketWrapper> partitionedStream =
                bucketAssignedStream.partitionCustom(
                        (key, numPartitions) -> key % numPartitions,
                        (KeySelector<BucketWrapper, Integer>) BucketWrapper::getBucket);

        // Step 3: Flush event alignment
        // - Aligns FlushEvents from multiple BucketAssignOperator instances
        // - Ensures each writer receives only one FlushEvent per source
        DataStream<BucketWrapper> alignedStream =
                partitionedStream
                        .transform(
                                "flush_event_alignment",
                                TypeInformation.of(BucketWrapper.class),
                                new FlushEventAlignmentOperator())
                        .uid("flush_event_alignment");

        // Step 4: Unwrap and write to Hudi
        // Use map to unwrap BucketWrapper before passing to MultiTableWriteOperator
        DataStream<Event> unwrappedStream =
                alignedStream.map(wrapper -> wrapper.getEvent(), TypeInformation.of(Event.class));

        return unwrappedStream
                .transform(
                        "multi_table_write",
                        TypeInformation.of(RowData.class),
                        MultiTableWriteOperator.getFactory(conf, schemaOperatorUid))
                .uid("multi_table_write")
                .flatMap(
                        (RowData rowData, Collector<Event> out) -> {
                            // Write side effects are handled by the operator, no events emitted
                            // downstream
                        })
                .returns(TypeInformation.of(Event.class));
    }

    /** Dummy sink writer that does nothing. */
    private static class DummySinkWriter implements SinkWriter<Event> {
        private static final SinkWriter<Event> INSTANCE = new DummySinkWriter();

        @Override
        public void write(Event element, Context context) {
            // do nothing
        }

        @Override
        public void flush(boolean endOfInput) {
            // do nothing
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
