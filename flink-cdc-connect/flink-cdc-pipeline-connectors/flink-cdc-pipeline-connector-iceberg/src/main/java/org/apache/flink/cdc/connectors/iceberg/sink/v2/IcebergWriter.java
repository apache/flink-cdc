package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.connector.sink2.CommittingSinkWriter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.flink.FlinkSchemaUtil;
import org.apache.iceberg.flink.sink.RowDataTaskWriterFactory;
import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.util.PropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergWriter<InputT> implements CommittingSinkWriter<InputT, WriteResult> {

    private static final Logger LOG = LoggerFactory.getLogger(IcebergWriter.class);

    private final Table table;
    private final String commitUser;
    private final MetricGroup metricGroup;
    private final Map<String, TaskWriter<RowData>> writes;
    private final IcebergRecordSerializer<InputT> serializer;

    private long lastCheckpointId;

    public IcebergWriter(
            Table table,
            MetricGroup metricGroup,
            String commitUser,
            IcebergRecordSerializer<InputT> serializer,
            long lastCheckpointId) {
        this.table = table;
        this.commitUser = commitUser;
        this.metricGroup = metricGroup;
        this.serializer = serializer;
        this.writes = new HashMap<>();
        this.lastCheckpointId = lastCheckpointId;
    }

    @Override
    public Collection<WriteResult> prepareCommit() throws IOException, InterruptedException {
        List<WriteResult> committables = new ArrayList<>();
        for (Map.Entry<String, TaskWriter<RowData>> entry : writes.entrySet()) {
            String key = entry.getKey();
            TaskWriter<RowData> writer = entry.getValue();
            WriteResult result = writer.complete();
            committables.add(result);
            writes.put(key, getTaskWriter());
            LOG.info(
                    "Iceberg writer flushed {} data files and {} delete files",
                    result.dataFiles().length,
                    result.deleteFiles().length);
        }
        return committables;
    }

    @Override
    public void write(InputT inputT, Context context) throws IOException, InterruptedException {
        IcebergEvent icebergEvent = serializer.serialize(inputT);
        String tableId = icebergEvent.getTableId().name();

        // Handle schema changes (if any)
        if (icebergEvent.isShouldRefreshSchema()) {
            // In case of schema changes, refresh the table
            try {
                table.refresh();
            } catch (Exception e) {
                throw new IOException("Failed to refresh Iceberg table schema", e);
            }
        }

        // Write the data to Iceberg
        if (icebergEvent.getGenericRow() != null) {
            TaskWriter<RowData> writer = writes.computeIfAbsent(tableId, id -> getTaskWriter());

            try {
                writer.write(icebergEvent.getGenericRow());
            } catch (Exception e) {
                throw new IOException("Failed to write event to Iceberg", e);
            }
        }
    }

    private TaskWriter<RowData> getTaskWriter() {
        String formatString =
                PropertyUtil.propertyAsString(
                        table.properties(),
                        TableProperties.DEFAULT_FILE_FORMAT,
                        TableProperties.DEFAULT_FILE_FORMAT_DEFAULT);
        FileFormat format = FileFormat.fromString(formatString);
        RowType flinkSchema = FlinkSchemaUtil.convert(table.schema());
        RowDataTaskWriterFactory taskWriterFactory =
                new RowDataTaskWriterFactory(
                        table, flinkSchema, Long.MAX_VALUE, format, table.properties(), null, true);
        return taskWriterFactory.create();
    }

    @Override
    public void flush(boolean b) throws IOException, InterruptedException {
        // flush is used to handle flush/endOfInput, so no action is taken here.
    }

    @Override
    public void writeWatermark(Watermark watermark) throws IOException, InterruptedException {}

    @Override
    public void close() throws Exception {
        for (TaskWriter<RowData> writer : writes.values()) {
            if (writer != null) {
                writer.close();
            }
        }
    }
}
