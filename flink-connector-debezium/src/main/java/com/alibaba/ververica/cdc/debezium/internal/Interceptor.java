package com.alibaba.ververica.cdc.debezium.internal;

import org.apache.flink.configuration.Configuration;

import java.io.Closeable;
import java.io.IOException;

public interface Interceptor extends Closeable {

    void init(Configuration configuration);

    void pre(Context context);

    void post(Context context);

    default void close() throws IOException {}

    interface Context extends Iterable<String> {
        void put(String key, Object value);

        int size();

        <T> T get(String key, Class<T> type);

        Object get(String key);
    }
}
