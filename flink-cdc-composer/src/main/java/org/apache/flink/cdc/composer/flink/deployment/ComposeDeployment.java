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

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.flink.cdc.common.utils.Preconditions.checkNotNull;

/** Create deployment methods corresponding to different goals. */
public enum ComposeDeployment {
    YARN_SESSION("yarn-session"),
    YARN_APPLICATION("yarn-application"),
    LOCAL("local"),
    REMOTE("remote"),
    KUBERNETES_APPLICATION("kubernetes-application");

    private final String name;

    ComposeDeployment(final String name) {
        this.name = checkNotNull(name);
    }

    public String getName() {
        return name;
    }

    public static ComposeDeployment getDeploymentFromName(final String deploymentTargetStr) {
        return Arrays.stream(ComposeDeployment.values())
                .filter(d -> d.name.equalsIgnoreCase(deploymentTargetStr))
                .findFirst()
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Unknown deployment target \""
                                                + deploymentTargetStr
                                                + "\"."
                                                + " The available options are: "
                                                + options()));
    }

    private static String options() {
        return Arrays.stream(ComposeDeployment.values())
                .map(ComposeDeployment::getName)
                .collect(Collectors.joining(","));
    }
}
