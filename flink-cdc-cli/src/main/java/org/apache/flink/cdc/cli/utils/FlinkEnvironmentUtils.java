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

package org.apache.flink.cdc.cli.utils;

import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.core.fs.Path;

import org.apache.flink.shaded.guava31.com.google.common.base.Joiner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

/** Utilities for handling Flink configuration and environment. */
public class FlinkEnvironmentUtils {

    private static final Logger LOG = LoggerFactory.getLogger(FlinkEnvironmentUtils.class);
    private static final String FLINK_CONF_DIR = "conf";
    private static final String LEGACY_FLINK_CONF_FILENAME = "flink-conf.yaml";
    private static final String FLINK_CONF_FILENAME = "config.yaml";

    public static Configuration loadFlinkConfiguration(Path flinkHome) throws Exception {
        Path flinkConfPath =
                new Path(
                        flinkHome,
                        Joiner.on(File.separator).join(FLINK_CONF_DIR, FLINK_CONF_FILENAME));
        if (flinkConfPath.getFileSystem().exists(flinkConfPath)) {
            return ConfigurationUtils.loadConfigFile(flinkConfPath);
        } else {
            return ConfigurationUtils.loadConfigFile(
                    new Path(
                            flinkHome,
                            Joiner.on(File.separator)
                                    .join(FLINK_CONF_DIR, LEGACY_FLINK_CONF_FILENAME)),
                    true);
        }
    }
}
