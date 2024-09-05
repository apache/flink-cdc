/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.oceanbase.table;

import org.apache.flink.cdc.connectors.oceanbase.OceanBaseTestBase;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseCdcMetadata;
import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseOracleCdcMetadata;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;

import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/** Integration tests for OceanBase Oracle mode table source. */
@Ignore("Test ignored before oceanbase-xe docker image is available")
public class OceanBaseOracleModeITCase extends OceanBaseTestBase {

    private final StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    private final StreamTableEnvironment tEnv =
            StreamTableEnvironment.create(
                    env, EnvironmentSettings.newInstance().inStreamingMode().build());

    private static final OceanBaseCdcMetadata METADATA = new OceanBaseOracleCdcMetadata();

    @Override
    protected OceanBaseCdcMetadata metadata() {
        return METADATA;
    }

    @Override
    protected String logProxyOptionsString() {
        return super.logProxyOptionsString()
                + " , "
                + String.format(" 'config-url' = '%s'", METADATA.getConfigUrl());
    }

    @Test
    public void testAllDataTypes() throws Exception {
        initializeTable("column_type_test");

        String schema = metadata().getDatabase();
        String sourceDDL =
                String.format(
                        "CREATE TABLE full_types ("
                                + " ID INT NOT NULL,"
                                + " VAL_VARCHAR STRING,"
                                + " VAL_VARCHAR2 STRING,"
                                + " VAL_NVARCHAR2 STRING,"
                                + " VAL_CHAR STRING,"
                                + " VAL_NCHAR STRING,"
                                + " VAL_BF FLOAT,"
                                + " VAL_BD DOUBLE,"
                                + " VAL_F FLOAT,"
                                + " VAL_F_10 FLOAT,"
                                + " VAL_NUM DECIMAL(10, 6),"
                                + " VAL_DP DOUBLE,"
                                + " VAL_R DECIMAL(38,2),"
                                + " VAL_DECIMAL DECIMAL(10, 6),"
                                + " VAL_NUMERIC DECIMAL(10, 6),"
                                + " VAL_NUM_VS DECIMAL(10, 3),"
                                + " VAL_INT DECIMAL(38,0),"
                                + " VAL_INTEGER DECIMAL(38,0),"
                                + " VAL_SMALLINT DECIMAL(38,0),"
                                + " VAL_NUMBER_38_NO_SCALE DECIMAL(38,0),"
                                + " VAL_NUMBER_38_SCALE_0 DECIMAL(38,0),"
                                + " VAL_NUMBER_1 BOOLEAN,"
                                + " VAL_NUMBER_2 TINYINT,"
                                + " VAL_NUMBER_4 SMALLINT,"
                                + " VAL_NUMBER_9 INT,"
                                + " VAL_NUMBER_18 BIGINT,"
                                + " VAL_NUMBER_2_NEGATIVE_SCALE TINYINT,"
                                + " VAL_NUMBER_4_NEGATIVE_SCALE SMALLINT,"
                                + " VAL_NUMBER_9_NEGATIVE_SCALE INT,"
                                + " VAL_NUMBER_18_NEGATIVE_SCALE BIGINT,"
                                + " VAL_NUMBER_36_NEGATIVE_SCALE DECIMAL(38,0),"
                                + " VAL_DATE TIMESTAMP,"
                                + " VAL_TS TIMESTAMP,"
                                + " VAL_TS_PRECISION2 TIMESTAMP(2),"
                                + " VAL_TS_PRECISION4 TIMESTAMP(4),"
                                + " VAL_TS_PRECISION9 TIMESTAMP(6),"
                                + " VAL_CLOB_INLINE STRING,"
                                + " VAL_BLOB_INLINE BYTES,"
                                + " PRIMARY KEY (ID) NOT ENFORCED"
                                + ") WITH ("
                                + initialOptionsString()
                                + ", "
                                + " 'table-list' = '%s'"
                                + ")",
                        schema + ".FULL_TYPES");

        String sinkDDL =
                "CREATE TABLE sink ("
                        + " ID INT,"
                        + " VAL_VARCHAR STRING,"
                        + " VAL_VARCHAR2 STRING,"
                        + " VAL_NVARCHAR2 STRING,"
                        + " VAL_CHAR STRING,"
                        + " VAL_NCHAR STRING,"
                        + " VAL_BF FLOAT,"
                        + " VAL_BD DOUBLE,"
                        + " VAL_F FLOAT,"
                        + " VAL_F_10 FLOAT,"
                        + " VAL_NUM DECIMAL(10, 6),"
                        + " VAL_DP DOUBLE,"
                        + " VAL_R DECIMAL(38,2),"
                        + " VAL_DECIMAL DECIMAL(10, 6),"
                        + " VAL_NUMERIC DECIMAL(10, 6),"
                        + " VAL_NUM_VS DECIMAL(10, 3),"
                        + " VAL_INT DECIMAL(38,0),"
                        + " VAL_INTEGER DECIMAL(38,0),"
                        + " VAL_SMALLINT DECIMAL(38,0),"
                        + " VAL_NUMBER_38_NO_SCALE DECIMAL(38,0),"
                        + " VAL_NUMBER_38_SCALE_0 DECIMAL(38,0),"
                        + " VAL_NUMBER_1 BOOLEAN,"
                        + " VAL_NUMBER_2 TINYINT,"
                        + " VAL_NUMBER_4 SMALLINT,"
                        + " VAL_NUMBER_9 INT,"
                        + " VAL_NUMBER_18 BIGINT,"
                        + " VAL_NUMBER_2_NEGATIVE_SCALE TINYINT,"
                        + " VAL_NUMBER_4_NEGATIVE_SCALE SMALLINT,"
                        + " VAL_NUMBER_9_NEGATIVE_SCALE INT,"
                        + " VAL_NUMBER_18_NEGATIVE_SCALE BIGINT,"
                        + " VAL_NUMBER_36_NEGATIVE_SCALE DECIMAL(38,0),"
                        + " VAL_DATE TIMESTAMP,"
                        + " VAL_TS TIMESTAMP,"
                        + " VAL_TS_PRECISION2 TIMESTAMP(2),"
                        + " VAL_TS_PRECISION4 TIMESTAMP(4),"
                        + " VAL_TS_PRECISION9 TIMESTAMP(6),"
                        + " VAL_CLOB_INLINE STRING,"
                        + " VAL_BLOB_INLINE STRING,"
                        + " PRIMARY KEY (ID) NOT ENFORCED"
                        + ") WITH ("
                        + " 'connector' = 'values',"
                        + " 'sink-insert-only' = 'false',"
                        + " 'sink-expected-messages-num' = '2'"
                        + ")";

        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink SELECT "
                                + " ID,"
                                + " VAL_VARCHAR,"
                                + " VAL_VARCHAR2,"
                                + " VAL_NVARCHAR2,"
                                + " VAL_CHAR,"
                                + " VAL_NCHAR,"
                                + " VAL_BF,"
                                + " VAL_BD,"
                                + " VAL_F,"
                                + " VAL_F_10,"
                                + " VAL_NUM,"
                                + " VAL_DP,"
                                + " VAL_R,"
                                + " VAL_DECIMAL,"
                                + " VAL_NUMERIC,"
                                + " VAL_NUM_VS,"
                                + " VAL_INT,"
                                + " VAL_INTEGER,"
                                + " VAL_SMALLINT,"
                                + " VAL_NUMBER_38_NO_SCALE,"
                                + " VAL_NUMBER_38_SCALE_0,"
                                + " VAL_NUMBER_1,"
                                + " VAL_NUMBER_2,"
                                + " VAL_NUMBER_4,"
                                + " VAL_NUMBER_9,"
                                + " VAL_NUMBER_18,"
                                + " VAL_NUMBER_2_NEGATIVE_SCALE,"
                                + " VAL_NUMBER_4_NEGATIVE_SCALE,"
                                + " VAL_NUMBER_9_NEGATIVE_SCALE,"
                                + " VAL_NUMBER_18_NEGATIVE_SCALE,"
                                + " VAL_NUMBER_36_NEGATIVE_SCALE,"
                                + " VAL_DATE,"
                                + " VAL_TS,"
                                + " VAL_TS_PRECISION2,"
                                + " VAL_TS_PRECISION4,"
                                + " VAL_TS_PRECISION9,"
                                + " VAL_CLOB_INLINE,"
                                + " DECODE(VAL_BLOB_INLINE, 'UTF-8')"
                                + " FROM full_types");

        waitForSinkSize("sink", 1);

        try (Connection connection = getJdbcConnection();
                Statement statement = connection.createStatement()) {
            statement.execute(
                    "UPDATE FULL_TYPES SET VAL_TS = '2022-10-30 12:34:56.12545' WHERE id=1;");
        }

        waitForSinkSize("sink", 2);

        List<String> expected =
                Arrays.asList(
                        "+I(1,vc2,vc2,nvc2,c  ,nc ,1.1,2.22,3.33,8.888,4.444400,5.555,6.66,1234.567891,1234.567891,77.323,1,22,333,4444,5555,true,99,9999,999999999,999999999999999999,90,9900,999999990,999999999999999900,99999999999999999999999999999999999900,2022-10-30T00:00,2022-10-30T12:34:56.007890,2022-10-30T12:34:56.130,2022-10-30T12:34:56.125500,2022-10-30T12:34:56.125457,col_clob,col_blob)",
                        "+U(1,vc2,vc2,nvc2,c  ,nc ,1.1,2.22,3.33,8.888,4.444400,5.555,6.66,1234.567891,1234.567891,77.323,1,22,333,4444,5555,true,99,9999,999999999,999999999999999999,90,9900,999999990,999999999999999900,99999999999999999999999999999999999900,2022-10-30T00:00,2022-10-30T12:34:56.125450,2022-10-30T12:34:56.130,2022-10-30T12:34:56.125500,2022-10-30T12:34:56.125457,col_clob,col_blob)");

        List<String> actual = TestValuesTableFactory.getRawResults("sink");
        assertContainsInAnyOrder(expected, actual);
        result.getJobClient().get().cancel().get();
    }
}
