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

package org.apache.flink.cdc.composer.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.net.URL;
import java.util.Collections;
import java.util.List;

/** Test for {@link FlinkEnvironmentUtils}. */
public class FlinkEnvironmentUtilsTest {

    @Test
    public void testAddJars() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(PipelineOptions.JARS, Collections.EMPTY_LIST);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.createLocalEnvironment(configuration);

        FlinkEnvironmentUtils.addJar(
                env, Lists.newArrayList(new URL("file://a.jar"), new URL("file://a.jar")));
        List<String> expectedJars = Lists.newArrayList("file://a.jar");
        Assert.assertEquals(expectedJars, env.getConfiguration().get(PipelineOptions.JARS));

        FlinkEnvironmentUtils.addJar(
                env, Lists.newArrayList(new URL("file://b.jar"), new URL("file://a.jar")));
        expectedJars.add("file://b.jar");
        Assert.assertEquals(expectedJars, env.getConfiguration().get(PipelineOptions.JARS));
    }
}
