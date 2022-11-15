/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.debezium.connector.mysql.util;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** The tests for {@link ErrorMessageUtils}. */
public class ErrorMessageUtilsTest {
    @Test
    public void testOptimizeErrorMessageWhenServerIdConflict() {
        assertEquals(
                "A slave with the same server_uuid/server_id as this slave has connected to the master Error code: 1236; SQLSTATE: HY000."
                        + "\nThe 'server-id' in the mysql cdc connector should be globally unique, but conflicts happen now.\n"
                        + "The server id conflict may happen in the following situations: \n"
                        + "1. The server id has been used by other mysql cdc table in the current job.\n"
                        + "2. The server id has been used by the mysql cdc table in other jobs.\n"
                        + "3. The server id has been used by other sync tools like canal, debezium and so on.\n",
                ErrorMessageUtils.optimizeErrorMessage(
                        "A slave with the same server_uuid/server_id as this slave has connected to the master Error code: 1236; SQLSTATE: HY000."));
    }

    @Test
    public void testOptimizeErrorMessageWhenMissingBinlogPositionInMaster() {
        assertEquals(
                "Cannot replicate because the master purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new slave from backup. Consider increasing the master's binary log expiration period. The GTID set sent by the slave is 'b9d6f3df-79e7-11ed-9a81-0242ac110004:1-33', and the missing transactions are 'b9d6f3df-79e7-11ed-9a81-0242ac110004:34'"
                        + "\nThe required binary logs are no longer available on the server. This may happen in following situations:\n"
                        + "1. The speed of CDC source reading is too slow to exceed the binlog expired period. You can consider increasing the binary log expiration period, you can also to check whether there is back pressure in the job and optimize your job.\n"
                        + "2. The job runs normally, but something happens in the database and lead to the binlog cleanup. You can try to check why this cleanup happens from MySQL side.",
                ErrorMessageUtils.optimizeErrorMessage(
                        "Cannot replicate because the master purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new slave from backup. Consider increasing the master's binary log expiration period. The GTID set sent by the slave is 'b9d6f3df-79e7-11ed-9a81-0242ac110004:1-33', and the missing transactions are 'b9d6f3df-79e7-11ed-9a81-0242ac110004:34'"));
    }

    @Test
    public void testOptimizeErrorMessageWhenMissingBinlogPositionInSource() {
        assertEquals(
                "Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new slave from backup. Consider increasing the master's binary log expiration period. The GTID set sent by the slave is 'b9d6f3df-79e7-11ed-9a81-0242ac110004:1-33', and the missing transactions are 'b9d6f3df-79e7-11ed-9a81-0242ac110004:34'"
                        + "\nThe required binary logs are no longer available on the server. This may happen in following situations:\n"
                        + "1. The speed of CDC source reading is too slow to exceed the binlog expired period. You can consider increasing the binary log expiration period, you can also to check whether there is back pressure in the job and optimize your job.\n"
                        + "2. The job runs normally, but something happens in the database and lead to the binlog cleanup. You can try to check why this cleanup happens from MySQL side.",
                ErrorMessageUtils.optimizeErrorMessage(
                        "Cannot replicate because the source purged required binary logs. Replicate the missing transactions from elsewhere, or provision a new slave from backup. Consider increasing the master's binary log expiration period. The GTID set sent by the slave is 'b9d6f3df-79e7-11ed-9a81-0242ac110004:1-33', and the missing transactions are 'b9d6f3df-79e7-11ed-9a81-0242ac110004:34'"));
    }

    @Test
    public void testOptimizeErrorMessageWhenMissingTransaction() {
        assertEquals(
                "The connector is trying to read binlog starting at Struct{version=1.6.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1670826084012,db=,server_id=0,file=mysql-bin.000005,pos=3845,row=0}, but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed."
                        + "\nThe required binary logs are no longer available on the server. This may happen in following situations:\n"
                        + "1. The speed of CDC source reading is too slow to exceed the binlog expired period. You can consider increasing the binary log expiration period, you can also to check whether there is back pressure in the job and optimize your job.\n"
                        + "2. The job runs normally, but something happens in the database and lead to the binlog cleanup. You can try to check why this cleanup happens from MySQL side.",
                ErrorMessageUtils.optimizeErrorMessage(
                        "The connector is trying to read binlog starting at Struct{version=1.6.4.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1670826084012,db=,server_id=0,file=mysql-bin.000005,pos=3845,row=0}, but this is no longer available on the server. Reconfigure the connector to use a snapshot when needed."));
    }
}
