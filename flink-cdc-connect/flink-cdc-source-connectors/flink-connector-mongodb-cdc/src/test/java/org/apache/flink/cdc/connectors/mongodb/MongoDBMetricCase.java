package org.apache.flink.cdc.connectors.mongodb;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.groups.InternalSourceReaderMetricGroup;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** IT tests for {@link MongoDBSource}. */
@RunWith(Parameterized.class)
public class MongoDBMetricCase extends MongoDBSourceTestBase {
    public static final Duration TIMEOUT = Duration.ofSeconds(300);

    public MongoDBMetricCase(String mongoVersion) {
        super(mongoVersion);
    }

    @Parameterized.Parameters(name = "mongoVersion: {0}")
    public static Object[] parameters() {
        return Stream.of(getMongoVersions()).map(e -> new Object[] {e}).toArray();
    }

    @Test
    public void testSourceMetrics() throws Exception {
        String customerDatabase = mongoContainer.executeCommandFileInSeparateDatabase("customer");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(200L);
        SourceFunction<String> sourceFunction =
                MongoDBSource.<String>builder()
                        .hosts(mongoContainer.getHostAndPort())
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .databaseList(customerDatabase)
                        .collectionList(
                                getCollectionNameRegex(
                                        customerDatabase, new String[] {"customers"}))
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();
        DataStreamSource<String> stream = env.addSource(sourceFunction, "MongoDB CDC Source");
        CollectResultIterator<String> iterator = addCollector(env, stream);
        JobClient jobClient = env.executeAsync();
        iterator.setJobClient(jobClient);

        //        // ---------------------------- Snapshot phase ------------------------------
        //        // Wait until we receive all 21 snapshot records
        int numSnapshotRecordsExpected = 21;
        int numSnapshotRecordsReceived = 0;

        while (numSnapshotRecordsReceived < numSnapshotRecordsExpected && iterator.hasNext()) {
            iterator.next();
            numSnapshotRecordsReceived++;
        }

        // Check metrics
        List<OperatorMetricGroup> metricGroups =
                metricReporter.findOperatorMetricGroups(jobClient.getJobID(), "MongoDB CDC Source");

        // There should be only 1 parallelism of source, so it's safe to get the only group
        OperatorMetricGroup group = metricGroups.get(0);
        Map<String, Metric> metrics = metricReporter.getMetricsByGroup(group);

        // numRecordsOut
        assertEquals(
                numSnapshotRecordsExpected,
                group.getIOMetricGroup().getNumRecordsOutCounter().getCount());

        // currentEmitEventTimeLag should be UNDEFINED during snapshot phase
        assertTrue(metrics.containsKey(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG));
        Gauge<Long> currentEmitEventTimeLag =
                (Gauge<Long>) metrics.get(MetricNames.CURRENT_EMIT_EVENT_TIME_LAG);
        assertEquals(
                InternalSourceReaderMetricGroup.UNDEFINED,
                (long) currentEmitEventTimeLag.getValue());
        // currentFetchEventTimeLag should be UNDEFINED during snapshot phase
        assertTrue(metrics.containsKey(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG));
        Gauge<Long> currentFetchEventTimeLag =
                (Gauge<Long>) metrics.get(MetricNames.CURRENT_FETCH_EVENT_TIME_LAG);
        assertEquals(
                InternalSourceReaderMetricGroup.UNDEFINED,
                (long) currentFetchEventTimeLag.getValue());
        // sourceIdleTime should be positive (we can't know the exact value)
        assertTrue(metrics.containsKey(MetricNames.SOURCE_IDLE_TIME));
        Gauge<Long> sourceIdleTime = (Gauge<Long>) metrics.get(MetricNames.SOURCE_IDLE_TIME);
        assertTrue(sourceIdleTime.getValue() > 0);
        assertTrue(sourceIdleTime.getValue() < TIMEOUT.toMillis());

        // --------------------------------- Binlog phase -----------------------------
        makeFirstPartChangeStreamEvents(mongodbClient.getDatabase(customerDatabase), "customers");
        // Wait until we receive 4 changes made above
        int numBinlogRecordsExpected = 4;
        int numBinlogRecordsReceived = 0;
        while (numBinlogRecordsReceived < numBinlogRecordsExpected && iterator.hasNext()) {
            iterator.next();
            numBinlogRecordsReceived++;
        }

        // Check metrics
        // numRecordsOut
        assertEquals(
                numSnapshotRecordsExpected + numBinlogRecordsExpected,
                group.getIOMetricGroup().getNumRecordsOutCounter().getCount());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(currentEmitEventTimeLag.getValue() > 0);
        assertTrue(currentEmitEventTimeLag.getValue() < TIMEOUT.toMillis());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(currentFetchEventTimeLag.getValue() > 0);
        assertTrue(currentFetchEventTimeLag.getValue() < TIMEOUT.toMillis());

        // currentEmitEventTimeLag should be reasonably positive (we can't know the exact value)
        assertTrue(sourceIdleTime.getValue() > 0);
        assertTrue(sourceIdleTime.getValue() < TIMEOUT.toMillis());

        jobClient.cancel().get();
        iterator.close();
    }

    private <T> CollectResultIterator<T> addCollector(
            StreamExecutionEnvironment env, DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getTransformation().getOutputType().createSerializer(env.getConfig()); //
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig(),
                        10000L);
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        return iterator;
    }

    private void makeFirstPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 101L), Updates.set("address", "Hangzhou"));
        mongoCollection.deleteOne(Filters.eq("cid", 102L));
        mongoCollection.insertOne(customerDocOf(102L, "user_2", "Shanghai", "123567891234"));
        mongoCollection.updateOne(Filters.eq("cid", 103L), Updates.set("address", "Hangzhou"));
    }

    private Document customerDocOf(Long cid, String name, String address, String phoneNumber) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("name", name);
        document.put("address", address);
        document.put("phone_number", phoneNumber);
        return document;
    }

    private String getCollectionNameRegex(String database, String[] captureCustomerCollections) {
        checkState(captureCustomerCollections.length > 0);
        if (captureCustomerCollections.length == 1) {
            return database + "." + captureCustomerCollections[0];
        } else {
            // pattern that matches multiple collections
            return Arrays.stream(captureCustomerCollections)
                    .map(coll -> "^(" + database + "." + coll + ")$")
                    .collect(Collectors.joining("|"));
        }
    }
}
