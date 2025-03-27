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

package org.apache.flink.cdc.pipeline.tests.utils;

import org.apache.flink.cdc.common.test.utils.TestUtils;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;

/** Obtain and downloads corresponding Flink CDC tarball files. */
public abstract class TarballFetcher {

    private static final Logger LOG = LoggerFactory.getLogger(TarballFetcher.class);

    public static void fetchAll(GenericContainer<?> container) throws Exception {
        fetch(container, CdcVersion.values());
    }

    public static void fetchLatest(GenericContainer<?> container) throws Exception {
        fetch(container, CdcVersion.SNAPSHOT);
    }

    public static void fetch(GenericContainer<?> container, CdcVersion... versions)
            throws Exception {
        for (CdcVersion version : versions) {
            TarballFetcher.fetchInternal(container, version);
        }
    }

    private static void fetchInternal(GenericContainer<?> container, CdcVersion version)
            throws Exception {
        LOG.info("Trying to download CDC tarball @ {}...", version);
        if (CdcVersion.SNAPSHOT.equals(version)) {
            LOG.info("CDC {} is a snapshot version, we should fetch it locally...", version);

            container.copyFileToContainer(
                    MountableFile.forHostPath(
                            TestUtils.getResource("flink-cdc.sh", "flink-cdc-dist", "src"), 755),
                    version.workDir() + "/bin/flink-cdc.sh");
            container.copyFileToContainer(
                    MountableFile.forHostPath(
                            TestUtils.getResource("flink-cdc.yaml", "flink-cdc-dist", "src"), 755),
                    version.workDir() + "/conf/flink-cdc.yaml");
            container.copyFileToContainer(
                    MountableFile.forHostPath(TestUtils.getResource("flink-cdc-dist.jar")),
                    version.workDir() + "/lib/flink-cdc-dist.jar");
            container.copyFileToContainer(
                    MountableFile.forHostPath(
                            TestUtils.getResource("mysql-cdc-pipeline-connector.jar")),
                    version.workDir() + "/lib/mysql-cdc-pipeline-connector.jar");
            container.copyFileToContainer(
                    MountableFile.forHostPath(
                            TestUtils.getResource("values-cdc-pipeline-connector.jar")),
                    version.workDir() + "/lib/values-cdc-pipeline-connector.jar");

        } else {
            LOG.info("CDC {} is a released version, download it from the Internet...", version);

            String containerPath = "/tmp/tarball/" + version.getVersion() + ".tar.gz";
            downloadAndCopyToContainer(container, version.tarballUrl(), containerPath);
            container.execInContainer("mkdir", "-p", version.workDir());
            container.execInContainer(
                    "tar", "-xzvf", containerPath, "-C", version.workDir(), "--strip-components=1");

            downloadAndCopyToContainer(
                    container,
                    version.connectorJarUrl("mysql"),
                    version.workDir() + "/lib/mysql-cdc-pipeline-connector.jar");
            downloadAndCopyToContainer(
                    container,
                    version.connectorJarUrl("values"),
                    version.workDir() + "/lib/values-cdc-pipeline-connector.jar");
        }
    }

    private static void downloadAndCopyToContainer(
            GenericContainer<?> container, String url, String containerPath) throws Exception {
        Path tempFile = Files.createTempFile("download-", ".tmp");
        FileUtils.copyURLToFile(
                new URL(url),
                tempFile.toFile(),
                (int) Duration.ofMinutes(1).toMillis(),
                (int) Duration.ofMinutes(5).toMillis());
        container.copyFileToContainer(MountableFile.forHostPath(tempFile), containerPath);
    }

    /** Enum for all released Flink CDC version tags. */
    public enum CdcVersion {
        V3_1_1("3.1.1"),
        V3_2_0("3.2.0"),
        V3_2_1("3.2.1"),
        V3_3_0("3.3.0"),
        SNAPSHOT("SNAPSHOT");

        private final String version;

        CdcVersion(String version) {
            this.version = version;
        }

        public String getVersion() {
            return version;
        }

        public static List<CdcVersion> getAllVersions() {
            return Arrays.asList(CdcVersion.values());
        }

        public static List<CdcVersion> getVersionsSince(CdcVersion version) {
            return getAllVersions()
                    .subList(getAllVersions().indexOf(version), getAllVersions().size());
        }

        public String tarballUrl() {
            return "https://dlcdn.apache.org/flink/flink-cdc-"
                    + version
                    + "/flink-cdc-"
                    + version
                    + "-bin.tar.gz";
        }

        public String workDir() {
            return "/tmp/cdc/" + version;
        }

        public String connectorJarUrl(String name) {
            return String.format(
                    "https://repo1.maven.org/maven2/org/apache/flink/flink-cdc-pipeline-connector-%s/%s/flink-cdc-pipeline-connector-%s-%s.jar",
                    name, version, name, version);
        }
    }
}
