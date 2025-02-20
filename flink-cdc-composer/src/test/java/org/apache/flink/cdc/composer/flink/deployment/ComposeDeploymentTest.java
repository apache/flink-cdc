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

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

/** Test for {@link ComposeDeployment}. */
public class ComposeDeploymentTest {
    @Test
    public void testComposeDeployment() throws Exception {
        Assertions.assertThat(ComposeDeployment.getDeploymentFromName("yarn-application"))
                .as("test yarn-application")
                .isEqualTo(ComposeDeployment.YARN_APPLICATION);

        Assertions.assertThat(ComposeDeployment.getDeploymentFromName("yarn-Application"))
                .as("test ignore case")
                .isEqualTo(ComposeDeployment.YARN_APPLICATION);

        Assertions.assertThatThrownBy(() -> ComposeDeployment.getDeploymentFromName("unKnown"))
                .as("test Unknown deployment target")
                .hasMessage(
                        "Unknown deployment target \"unKnown\". The available options are: yarn-session,yarn-application,local,remote,kubernetes-application");
    }
}
