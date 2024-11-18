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

package org.apache.flink.cdc.connectors.utils;

import org.apache.flink.test.util.AbstractTestBase;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;

import java.io.File;

/** Wrapper class for JUnit-4 style Flink's {@link AbstractTestBase}. */
public class AbstractTestBaseProxy extends AbstractTestBase {

    @BeforeAll
    @SuppressWarnings("JUnitMixedFramework")
    static void startMiniCluster() throws Exception {
        MINI_CLUSTER_RESOURCE.before();
    }

    @AfterAll
    @SuppressWarnings("JUnitMixedFramework")
    static void stopMiniCluster() {
        MINI_CLUSTER_RESOURCE.after();
    }

    @AfterEach
    @SuppressWarnings("JUnitMixedFramework")
    void cleanup() throws Exception {
        super.cleanupRunningJobs();
    }

    @Override
    public String getTempFilePath(String fileName) {
        throw new UnsupportedOperationException("Please use JUnit 5 @TempDir annotations instead.");
    }

    @Override
    public String getTempDirPath(String dirName) {
        throw new UnsupportedOperationException("Please use JUnit 5 @TempDir annotations instead.");
    }

    @Override
    public String createTempFile(String fileName, String contents) {
        throw new UnsupportedOperationException("Please use JUnit 5 @TempDir annotations instead.");
    }

    @Override
    public File createAndRegisterTempFile(String fileName) {
        throw new UnsupportedOperationException("Please use JUnit 5 @TempDir annotations instead.");
    }
}
