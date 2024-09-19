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

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** Utilities for migration tests. */
public class MigrationTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MigrationTestBase.class);

    /** Flink CDC versions since 3.0. */
    public enum FlinkCdcVersion {
        v3_0_0,
        v3_0_1,
        v3_1_0,
        v3_1_1,
        SNAPSHOT;

        public String getShadedClassPrefix() {
            switch (this) {
                case v3_0_0:
                    return "com.ververica.cdc.v3_0_0";
                case v3_0_1:
                    return "com.ververica.cdc.v3_0_1";
                case v3_1_0:
                    return "org.apache.flink.cdc.v3_1_0";
                case v3_1_1:
                    return "org.apache.flink.cdc.v3_1_1";
                case SNAPSHOT:
                    return "org.apache.flink.cdc.snapshot";
                default:
                    throw new RuntimeException("Unknown Flink CDC version: " + this);
            }
        }
    }

    private static final List<FlinkCdcVersion> versions =
            Arrays.asList(
                    FlinkCdcVersion.v3_0_0,
                    FlinkCdcVersion.v3_0_1,
                    FlinkCdcVersion.v3_1_0,
                    FlinkCdcVersion.v3_1_1,
                    FlinkCdcVersion.SNAPSHOT);

    public static List<FlinkCdcVersion> getAllVersions() {
        return versions.subList(0, versions.size());
    }

    public static List<FlinkCdcVersion> getVersionSince(FlinkCdcVersion sinceVersion) {
        return versions.subList(versions.indexOf(sinceVersion), versions.size());
    }

    public static List<FlinkCdcVersion> getAllVersionExcept(FlinkCdcVersion... excludedVersions) {
        List<FlinkCdcVersion> excluded = Arrays.asList(excludedVersions);
        return versions.stream().filter(e -> !excluded.contains(e)).collect(Collectors.toList());
    }

    public static FlinkCdcVersion getSnapshotVersion() {
        return versions.get(versions.size() - 1);
    }

    private static Class<?> getMockClass(FlinkCdcVersion version, String caseName)
            throws Exception {
        return Class.forName(version.getShadedClassPrefix() + ".migration.tests." + caseName);
    }

    protected void testMigrationFromTo(
            FlinkCdcVersion fromVersion, FlinkCdcVersion toVersion, String caseName)
            throws Exception {

        LOG.info("Testing {} compatibility case from {} -> {}", caseName, fromVersion, toVersion);

        // Serialize dummy object to bytes in early versions
        Class<?> fromVersionMockClass = getMockClass(fromVersion, caseName);
        Object fromVersionMockObject = fromVersionMockClass.newInstance();

        int serializerVersion =
                (int)
                        fromVersionMockClass
                                .getDeclaredMethod("getSerializerVersion")
                                .invoke(fromVersionMockObject);
        byte[] serializedObject =
                (byte[])
                        fromVersionMockClass
                                .getDeclaredMethod("serializeObject")
                                .invoke(fromVersionMockObject);

        // Deserialize object in latest versions
        Class<?> toVersionMockClass = getMockClass(toVersion, caseName);
        Object toVersionMockObject = toVersionMockClass.newInstance();

        Assert.assertTrue(
                (boolean)
                        toVersionMockClass
                                .getDeclaredMethod(
                                        "deserializeAndCheckObject", int.class, byte[].class)
                                .invoke(toVersionMockObject, serializerVersion, serializedObject));
    }
}
