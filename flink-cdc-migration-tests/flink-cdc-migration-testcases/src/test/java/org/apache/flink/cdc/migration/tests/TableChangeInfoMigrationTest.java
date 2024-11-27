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

package org.apache.flink.cdc.migration.tests;

import org.apache.flink.cdc.runtime.operators.transform.TableChangeInfo;

import org.junit.Test;

import static org.apache.flink.cdc.migration.tests.MigrationTestBase.FlinkCdcVersion.v3_0_0;
import static org.apache.flink.cdc.migration.tests.MigrationTestBase.FlinkCdcVersion.v3_0_1;
import static org.apache.flink.cdc.migration.tests.MigrationTestBase.FlinkCdcVersion.v3_1_0;

/** Migration test cases for {@link TableChangeInfo}. */
public class TableChangeInfoMigrationTest extends MigrationTestBase {

    public static String mockCaseName = "TableChangeInfoMigrationMock";

    @Test
    public void testMigration() throws Exception {
        // Transform feature does not present until 3.1.0, and
        // CDC 3.1.0 breaks backwards compatibility.
        for (FlinkCdcVersion version : getAllVersionExcept(v3_0_0, v3_0_1, v3_1_0)) {
            testMigrationFromTo(version, getSnapshotVersion(), mockCaseName);
        }
    }
}
