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

package org.apache.flink.cdc.connectors.oceanbase;

import org.apache.flink.cdc.connectors.oceanbase.testutils.OceanBaseContainer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.time.Duration;

/** Utils to help test. */
@SuppressWarnings("resource")
public class OceanBaseTestUtils {

    private static final Logger LOG = LoggerFactory.getLogger(OceanBaseTestUtils.class);

    private static final String OB_4_3_3_VERSION = "4.3.3.0-100000142024101215";

    private static final String SYS_PASSWORD = "123456";
    private static final String TEST_PASSWORD = "654321";

    public static OceanBaseContainer createOceanBaseContainerForJdbc() {
        return createOceanBaseContainer(OB_4_3_3_VERSION, "mini")
                .withStartupTimeout(Duration.ofMinutes(4));
    }

    public static OceanBaseContainer createOceanBaseContainer(String version, String mode) {
        return new OceanBaseContainer(version)
                .withMode(mode)
                .withTenantPassword(TEST_PASSWORD)
                .withEnv("OB_DATAFILE_SIZE", "2G")
                .withEnv("OB_LOG_DISK_SIZE", "4G")
                .withLogConsumer(new Slf4jLogConsumer(LOG));
    }
}
