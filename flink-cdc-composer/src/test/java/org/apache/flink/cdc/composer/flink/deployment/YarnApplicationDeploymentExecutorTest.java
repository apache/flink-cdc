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

package org.apache.flink.cdc.composer.flink.deployment;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link YarnApplicationDeploymentExecutor}. */
class YarnApplicationDeploymentExecutorTest {

    private static final String CONNECTOR_JAR = "flink-cdc-pipeline-connector-mysql-3.6.0.jar";

    @ParameterizedTest
    @ValueSource(
            strings = {
                "flink-cdc-dist-3.5.0.jar",
                "flink-cdc-dist-3.7-SNAPSHOT.jar",
                // Binaries released since 3.6.0 carry a Flink version suffix.
                "flink-cdc-dist-3.6.0-1.20.jar",
                "flink-cdc-dist-3.7.0-2.0.jar"
            })
    void testFindDistJarWhateverVersionItCarries(String distJarName, @TempDir Path libDir)
            throws Exception {
        Files.createFile(libDir.resolve(distJarName));
        // Connector jars share the same directory and must not be picked up.
        Files.createFile(libDir.resolve(CONNECTOR_JAR));
        // A directory named like the dist jar must not be picked up either.
        Files.createDirectory(libDir.resolve("flink-cdc-dist-directory.jar"));

        assertThat(YarnApplicationDeploymentExecutor.getFlinkCDCDistJar(toFlinkPath(libDir)))
                .endsWith(distJarName);
    }

    @Test
    void testDistJarNotFound(@TempDir Path libDir) throws Exception {
        Files.createFile(libDir.resolve(CONNECTOR_JAR));

        assertThatThrownBy(
                        () ->
                                YarnApplicationDeploymentExecutor.getFlinkCDCDistJar(
                                        toFlinkPath(libDir)))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("Failed to fetch Flink CDC dist jar");
    }

    private static org.apache.flink.core.fs.Path toFlinkPath(Path path) {
        return new org.apache.flink.core.fs.Path(path.toUri());
    }
}
