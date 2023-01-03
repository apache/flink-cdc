package com.ververica.cdc.connectors.mysql.debezium.task;

import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
import com.ververica.cdc.connectors.mysql.debezium.task.context.SimpleBinlogChangeEventSourceContextImpl;
import com.ververica.cdc.connectors.mysql.source.offset.BinlogOffset;
import io.debezium.connector.mysql.*;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import java.util.Map;

/**
 * Task to read  binlog file for seek binlog/position by specified timestamp
 */
public class SimpleBinlogReadTask extends MySqlStreamingChangeEventSource {

    private ChangeEventSourceContext context;

    private long timestamp;

    private Map<String, String> binlogOffset;

    private String currentBinlogFileName;

    private boolean isFirstEvent = true;

    private boolean isFound = false;

    public SimpleBinlogReadTask(MySqlConnectorConfig connectorConfig,
                                MySqlConnection connection,
                                EventDispatcher<TableId> dispatcher,
                                ErrorHandler errorHandler,
                                Clock clock,
                                MySqlTaskContext taskContext,
                                MySqlStreamingChangeEventSourceMetrics metrics,
                                long timestamp,
                                Map<String, String> binlogOffset) {
        super(connectorConfig, connection, dispatcher, errorHandler, clock, taskContext, metrics);
        this.binlogOffset = binlogOffset;
        this.timestamp = timestamp;
    }

    @Override
    public void execute(ChangeEventSourceContext context, MySqlOffsetContext offsetContext)
            throws InterruptedException {
        this.context = context;
        this.currentBinlogFileName = offsetContext.getSource().binlogFilename();
        super.execute(context, offsetContext);
    }

    @Override
    protected void handleEvent(MySqlOffsetContext offsetContext, Event event) {
        //if this event is the first event and its timestamp > this.timestamp,then stop and read next binlog file
        if (this.isFirstEvent) {
            if (event.getHeader().getTimestamp() > this.timestamp) {
                ((SimpleBinlogChangeEventSourceContextImpl) this.context).finished();
                return;
            } else {
                this.isFirstEvent = false;
            }
        }
        if (event.getHeader().getEventType() == EventType.ROTATE || event.getHeader().getEventType() == EventType.STOP) {
            ((SimpleBinlogChangeEventSourceContextImpl) this.context).finished();
            return;
        }
        if (this.isFound && event.getHeader().getTimestamp() > this.timestamp) {
            ((SimpleBinlogChangeEventSourceContextImpl) this.context).finished();
            return;
        }
        if (event.getHeader().getEventType() == EventType.XID) {
            if (event.getHeader().getTimestamp() <= this.timestamp) {
                this.isFound = true;
                binlogOffset.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, this.currentBinlogFileName);
                binlogOffset.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, String.valueOf(((EventHeaderV4)event.getHeader()).getNextPosition()));
            } else {
                return;
            }
        }
    }

}
