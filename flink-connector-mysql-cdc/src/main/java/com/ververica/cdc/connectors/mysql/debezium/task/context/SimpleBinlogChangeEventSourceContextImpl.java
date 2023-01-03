package com.ververica.cdc.connectors.mysql.debezium.task.context;

import io.debezium.pipeline.source.spi.ChangeEventSource;

/** The {@link ChangeEventSource.ChangeEventSourceContext} implementation for simple binlog task. */
public class SimpleBinlogChangeEventSourceContextImpl
        implements ChangeEventSource.ChangeEventSourceContext {

    private boolean currentTaskRunning = true;

    public void finished() {
        currentTaskRunning = false;
    }

    @Override
    public boolean isRunning() {
        return currentTaskRunning;
    }
}
