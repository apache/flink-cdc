package org.apache.flink.cdc.connectors.fluss.config;

import java.util.Map;

public class FlussSinkOptions {
    private final String bootstrapServers;
    private final String database;
    private final String table;
    private final Map<String, String> options;

    public FlussSinkOptions(
            String bootstrapServers, String database, String table, Map<String, String> options) {
        this.bootstrapServers = bootstrapServers;
        this.database = database;
        this.table = table;
        this.options = options;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public String getDatabase() {
        return database;
    }

    public String getTable() {
        return table;
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
