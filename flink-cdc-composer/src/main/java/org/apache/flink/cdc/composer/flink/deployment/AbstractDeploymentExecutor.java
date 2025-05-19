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

import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.composer.PipelineDeploymentExecutor;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.Path;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/** Abstract Deployment Executor. */
public abstract class AbstractDeploymentExecutor implements PipelineDeploymentExecutor {
    private static final String FLINK_CDC_HOME_ENV_VAR = "FLINK_CDC_HOME";
    private static final String FLINK_CDC_DIST_JAR_PATTERN =
            "^flink-cdc-dist-(\\d+(\\.\\d+)*)(-SNAPSHOT)?\\.jar$";
    private static final String CDC_MAIN_CLASS = "org.apache.flink.cdc.cli.CliExecutor";

    /** Get the Flink CDC dist jar from FLINK_CDC_HOME. */
    public String getFlinkCDCDistJarFromEnv() throws IOException {
        String flinkCDCHomeFromEnvVar = System.getenv(FLINK_CDC_HOME_ENV_VAR);
        Preconditions.checkNotNull(
                flinkCDCHomeFromEnvVar,
                "FLINK_CDC_HOME is not correctly set in environment variable, current FLINK_CDC_HOME is: "
                        + FLINK_CDC_HOME_ENV_VAR);
        Path flinkCDCLibPath = new Path(flinkCDCHomeFromEnvVar, "lib");
        if (!flinkCDCLibPath.getFileSystem().exists(flinkCDCLibPath)
                || !flinkCDCLibPath.getFileSystem().getFileStatus(flinkCDCLibPath).isDir()) {
            throw new RuntimeException(
                    "Flink cdc home lib is not file or not directory: "
                            + flinkCDCLibPath.makeQualified(flinkCDCLibPath.getFileSystem()));
        }

        FileStatus[] fileStatuses = flinkCDCLibPath.getFileSystem().listStatus(flinkCDCLibPath);
        Optional<Path> distJars =
                Arrays.stream(fileStatuses)
                        .filter(status -> !status.isDir())
                        .map(FileStatus::getPath)
                        .filter(path -> path.getName().matches(FLINK_CDC_DIST_JAR_PATTERN))
                        .findFirst();

        if (distJars.isPresent()) {
            Path path = distJars.get().makeQualified(distJars.get().getFileSystem());
            return path.toString();
        } else {
            throw new FileNotFoundException(
                    "Failed to fetch Flink CDC dist jar from path: " + flinkCDCLibPath);
        }
    }

    /**
     * Get the main class of Flink CDC.
     *
     * @return the main class of Flink CDC.
     */
    public String getCDCMainClass() {
        return CDC_MAIN_CLASS;
    }
}
