package com.ververica.cdc.connectors.mongodb;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

public class MongoDBSourceExampleTest {

    public static void main(String[] args) throws Exception {
        SourceFunction<String> sourceFunction =
                MongoDBSource.<String>builder()
                        .hosts("127.0.0.1:27017")
                        .username("mongouser")
                        .password("mongopw")
                        .database("mgdb")
                        .collection("orders")
                        .deserializer(new JsonDebeziumDeserializationSchema())
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .print()
                .setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();
    }
}
