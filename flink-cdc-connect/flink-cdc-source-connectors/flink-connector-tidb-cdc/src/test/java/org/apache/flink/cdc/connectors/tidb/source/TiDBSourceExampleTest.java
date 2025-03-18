package org.apache.flink.cdc.connectors.tidb.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import org.apache.flink.cdc.connectors.tidb.TiDBTestBase;
import org.apache.flink.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.flink.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.RowRowConverter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.CloseableIterator;

import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for TiDB Source based on incremental snapshot framework . */
public class TiDBSourceExampleTest extends TiDBTestBase {

    private static final String databaseName = "inventory";
    private static final String tableName = "products";

    @Test
    @Ignore
    public void testConsumingScanEvents() throws Exception {
        final DataType dataType =
                DataTypes.ROW(
                        DataTypes.FIELD("id", DataTypes.BIGINT()),
                        DataTypes.FIELD("name", DataTypes.STRING()),
                        DataTypes.FIELD("description", DataTypes.STRING()),
                        DataTypes.FIELD("weight", DataTypes.FLOAT()));

        initializeTidbTable("inventory");

        JdbcIncrementalSource<RowData> TiDBIncrementalSource =
                TiDBSourceBuilder.TiDBIncrementalSource.<RowData>builder()
                        .hostname(TIDB.getHost())
                        .port(TIDB.getMappedPort(TIDB_PORT))
                        .username(TiDBTestBase.TIDB_USER)
                        .password(TiDBTestBase.TIDB_PASSWORD)
                        .databaseList(databaseName)
                        .tableList(this.databaseName + "." + this.tableName)
                        .splitSize(10)
                        .deserializer(buildRowDataDebeziumDeserializeSchema(dataType))
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        CloseableIterator<RowData> iterator =
                env.fromSource(
                                TiDBIncrementalSource,
                                WatermarkStrategy.noWatermarks(),
                                "TiDBParallelSource")
                        .setParallelism(2)
                        .executeAndCollect(); // collect record

        String[] snapshotExpectedRecords =
                new String[] {
                    "+I[101, scooter, Small 2-wheel scooter, 3.14]",
                    "+I[102, car battery, 12V car battery, 8.1]",
                    "+I[103, 12-pack drill bits, 12-pack of drill bits with sizes ranging from #40 to #3, 0.8]",
                    "+I[104, hammer, 12oz carpenter's hammer, 0.75]",
                    "+I[105, hammer, 14oz carpenter's hammer, 0.875]",
                    "+I[106, hammer, 16oz carpenter's hammer, 1.0]",
                    "+I[107, rocks, box of assorted rocks, 5.3]",
                    "+I[108, jacket, water resistent black wind breaker, 0.1]",
                    "+I[109, spare tire, 24 inch spare tire, 22.2]"
                };

        // step-1: consume snapshot data
        List<RowData> snapshotRowDataList = new ArrayList<>();
        for (int i = 0; i < snapshotExpectedRecords.length && iterator.hasNext(); i++) {
            snapshotRowDataList.add(iterator.next());
        }

        List<String> snapshotActualRecords = formatResult(snapshotRowDataList, dataType);
        assertEqualsInAnyOrder(Arrays.asList(snapshotExpectedRecords), snapshotActualRecords);
    }

    private DebeziumDeserializationSchema<RowData> buildRowDataDebeziumDeserializeSchema(
            DataType dataType) {
        LogicalType logicalType = TypeConversions.fromDataToLogicalType(dataType);
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(logicalType);
        return RowDataDebeziumDeserializeSchema.newBuilder()
                .setPhysicalRowType((RowType) dataType.getLogicalType())
                .setResultTypeInfo(typeInfo)
                .build();
    }

    private List<String> formatResult(List<RowData> records, DataType dataType) {
        RowRowConverter rowRowConverter = RowRowConverter.create(dataType);
        rowRowConverter.open(Thread.currentThread().getContextClassLoader());
        return records.stream()
                .map(rowRowConverter::toExternal)
                .map(Object::toString)
                .collect(Collectors.toList());
    }

    public static void assertEqualsInAnyOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEqualsInOrder(
                expected.stream().sorted().collect(Collectors.toList()),
                actual.stream().sorted().collect(Collectors.toList()));
    }

    public static void assertEqualsInOrder(List<String> expected, List<String> actual) {
        assertTrue(expected != null && actual != null);
        assertEquals(expected.size(), actual.size());
        assertArrayEquals(expected.toArray(new String[0]), actual.toArray(new String[0]));
    }
}
