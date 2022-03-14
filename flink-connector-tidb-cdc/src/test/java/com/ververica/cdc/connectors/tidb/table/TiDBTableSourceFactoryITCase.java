/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.tidb.table;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.catalog.ResolvedCatalogTable;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;

import com.ververica.cdc.connectors.tidb.TiDBTestBase;
import com.ververica.cdc.connectors.tidb.TiKVRichParallelSourceFunction;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

/** Integration tests for TiDB table source factory. */
public class TiDBTableSourceFactoryITCase extends TiDBTestBase {

    private static final ResolvedSchema SCHEMA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("aaa", DataTypes.INT().notNull()),
                            Column.physical("bbb", DataTypes.STRING().notNull()),
                            Column.physical("ccc", DataTypes.DOUBLE()),
                            Column.physical("ddd", DataTypes.DECIMAL(31, 18)),
                            Column.physical("eee", DataTypes.TIMESTAMP(3))),
                    new ArrayList<>(),
                    UniqueConstraint.primaryKey("pk", Arrays.asList("bbb", "aaa")));

    private static final ResolvedSchema SCHEMA_WITH_METADATA =
            new ResolvedSchema(
                    Arrays.asList(
                            Column.physical("id", DataTypes.BIGINT().notNull()),
                            Column.physical("name", DataTypes.STRING()),
                            Column.physical("count", DataTypes.DECIMAL(38, 18)),
                            Column.metadata("time", DataTypes.TIMESTAMP_LTZ(3), "op_ts", true),
                            Column.metadata(
                                    "database_name", DataTypes.STRING(), "database_name", true),
                            Column.metadata("table_name", DataTypes.STRING(), "table_name", true),
                            Column.metadata("op_ts", DataTypes.TIMESTAMP(), "op_ts", true)),
                    Collections.emptyList(),
                    UniqueConstraint.primaryKey("pk", Collections.singletonList("id")));

    private static final String MY_HOSTNAME = "tidb0:4000";
    private static final String MY_USERNAME = "root";
    private static final String MY_PASSWORD = "";
    private static final String MY_DATABASE = "inventory";
    private static final String MY_TABLE = "products";
    private static final String PD_ADDRESS = "pd0:2379";
    private static final Map<String, String> OPTIONS = new HashMap<>();

    @Test
    public void testCommonProperties() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        MY_HOSTNAME,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PD_ADDRESS,
                        StartupOptions.latest(),
                        OPTIONS);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testOptionalProperties() {
        Map<String, String> properties = getAllOptions();
        properties.put("tikv.grpc.timeout_in_ms", "20000");
        properties.put("tikv.grpc.scan_timeout_in_ms", "20000");
        properties.put("tikv.batch_get_concurrency", "4");
        properties.put("tikv.batch_put_concurrency", "4");
        properties.put("tikv.batch_scan_concurrency", "4");
        properties.put("tikv.batch_delete_concurrency", "4");

        // validation for source
        DynamicTableSource actualSource = createTableSource(properties);
        Map<String, String> options = new HashMap<>();
        options.put("tikv.grpc.timeout_in_ms", "20000");
        options.put("tikv.grpc.scan_timeout_in_ms", "20000");
        options.put("tikv.batch_get_concurrency", "4");
        options.put("tikv.batch_put_concurrency", "4");
        options.put("tikv.batch_scan_concurrency", "4");
        options.put("tikv.batch_delete_concurrency", "4");
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA,
                        MY_HOSTNAME,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PD_ADDRESS,
                        StartupOptions.latest(),
                        options);
        assertEquals(expectedSource, actualSource);
    }

    @Test
    public void testMetadataColumns() {
        Map<String, String> properties = getAllOptions();

        // validation for source
        DynamicTableSource actualSource = createTableSource(SCHEMA_WITH_METADATA, properties);
        TiDBTableSource tidbTableSource = (TiDBTableSource) actualSource;
        tidbTableSource.applyReadableMetadata(
                Arrays.asList("op_ts", "database_name", "table_name"),
                SCHEMA_WITH_METADATA.toSourceRowDataType());
        actualSource = tidbTableSource.copy();
        TiDBTableSource expectedSource =
                new TiDBTableSource(
                        SCHEMA_WITH_METADATA,
                        MY_HOSTNAME,
                        MY_DATABASE,
                        MY_TABLE,
                        MY_USERNAME,
                        MY_PASSWORD,
                        PD_ADDRESS,
                        StartupOptions.latest(),
                        OPTIONS);
        expectedSource.producedDataType = SCHEMA_WITH_METADATA.toSourceRowDataType();
        expectedSource.metadataKeys = Arrays.asList("op_ts", "database_name", "table_name");

        assertEquals(expectedSource, actualSource);

        ScanTableSource.ScanRuntimeProvider provider =
                tidbTableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE);
        TiKVRichParallelSourceFunction<RowData> sourceFunction =
                (TiKVRichParallelSourceFunction<RowData>)
                        ((SourceFunctionProvider) provider).createSourceFunction();
        assertProducedTypeOfSourceFunction(sourceFunction, expectedSource.producedDataType);
    }

    private Map<String, String> getAllOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "tidb-cdc");
        options.put("hostname", MY_HOSTNAME);
        options.put("database-name", MY_DATABASE);
        options.put("table-name", MY_TABLE);
        options.put("username", MY_USERNAME);
        options.put("password", MY_PASSWORD);
        options.put("pd-addresses", PD_ADDRESS);
        options.put("scan.startup.mode", "latest-offset");
        return options;
    }

    private static DynamicTableSource createTableSource(
            ResolvedSchema schema, Map<String, String> options) {
        return FactoryUtil.createTableSource(
                null,
                ObjectIdentifier.of("default", "default", "t1"),
                new ResolvedCatalogTable(
                        CatalogTable.of(
                                Schema.newBuilder().fromResolvedSchema(schema).build(),
                                "mock source",
                                new ArrayList<>(),
                                options),
                        schema),
                new Configuration(),
                TiDBTableSourceFactoryITCase.class.getClassLoader(),
                false);
    }

    private static DynamicTableSource createTableSource(Map<String, String> options) {
        return createTableSource(SCHEMA, options);
    }

    private static void assertProducedTypeOfSourceFunction(
            TiKVRichParallelSourceFunction<RowData> sourceFunction, DataType expectedProducedType) {
        TypeInformation<RowData> producedType = sourceFunction.getProducedType();
        assertThat(producedType, instanceOf(InternalTypeInfo.class));
        InternalTypeInfo<RowData> rowDataInternalTypeInfo =
                (InternalTypeInfo<RowData>) producedType;
        DataType producedDataType = rowDataInternalTypeInfo.getDataType();
        assertEquals(expectedProducedType.toString(), producedDataType.toString());
    }
}
