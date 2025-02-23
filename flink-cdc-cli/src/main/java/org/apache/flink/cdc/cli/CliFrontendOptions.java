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

package org.apache.flink.cdc.cli;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/** Command line argument options for {@link CliFrontend}. */
public class CliFrontendOptions {
    public static final Option FLINK_HOME =
            Option.builder()
                    .longOpt("flink-home")
                    .hasArg()
                    .desc("Path of Flink home directory")
                    .build();

    public static final Option HELP =
            Option.builder("h").longOpt("help").desc("Display help message").build();

    public static final Option GLOBAL_CONFIG =
            Option.builder()
                    .longOpt("global-config")
                    .hasArg()
                    .desc("Path of the global configuration file for Flink CDC pipelines")
                    .build();

    public static final Option JAR =
            Option.builder()
                    .longOpt("jar")
                    .hasArgs()
                    .desc("JARs to be submitted together with the pipeline")
                    .build();

    public static final Option TARGET =
            Option.builder("t")
                    .longOpt("target")
                    .hasArg()
                    .desc(
                            "The deployment target for the execution. This can take one of the following values "
                                    + "local/remote/yarn-session/yarn-application/kubernetes-session/kubernetes"
                                    + "-application")
                    .build();

    public static final Option USE_MINI_CLUSTER =
            Option.builder()
                    .longOpt("use-mini-cluster")
                    .hasArg(false)
                    .desc("Use Flink MiniCluster to run the pipeline")
                    .build();

    public static final Option SAVEPOINT_PATH_OPTION =
            Option.builder("s")
                    .longOpt("from-savepoint")
                    .hasArg(true)
                    .desc(
                            "Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537")
                    .build();

    public static final Option SAVEPOINT_CLAIM_MODE =
            Option.builder("cm")
                    .longOpt("claim-mode")
                    .hasArg(true)
                    .desc(
                            "Defines how should we restore from the given savepoint. Supported options: "
                                    + "[claim - claim ownership of the savepoint and delete once it is"
                                    + " subsumed, no_claim (default) - do not claim ownership, the first"
                                    + " checkpoint will not reuse any files from the restored one, legacy "
                                    + "- the old behaviour, do not assume ownership of the savepoint files,"
                                    + " but can reuse some shared files")
                    .build();

    public static final Option SAVEPOINT_ALLOW_NON_RESTORED_OPTION =
            Option.builder("n")
                    .longOpt("allow-nonRestored-state")
                    .hasArg(false)
                    .desc(
                            "Allow to skip savepoint state that cannot be restored. "
                                    + "You need to allow this if you removed an operator from your "
                                    + "program that was part of the program when the savepoint was triggered.")
                    .build();

    public static final Option FLINK_CONFIG =
            Option.builder("D")
                    .required(false)
                    .numberOfArgs(2)
                    .valueSeparator('=')
                    .argName("Session dynamic flink config key=val")
                    .desc(
                            "Allows specifying multiple flink generic configuration options. The available"
                                    + "options can be found at https://nightlies.apache.org/flink/flink-docs-stable/ops/config.html")
                    .build();

    public static Options initializeOptions() {
        return new Options()
                .addOption(HELP)
                .addOption(JAR)
                .addOption(FLINK_HOME)
                .addOption(GLOBAL_CONFIG)
                .addOption(USE_MINI_CLUSTER)
                .addOption(TARGET)
                .addOption(SAVEPOINT_PATH_OPTION)
                .addOption(SAVEPOINT_CLAIM_MODE)
                .addOption(SAVEPOINT_ALLOW_NON_RESTORED_OPTION)
                .addOption(FLINK_CONFIG);
    }
}
