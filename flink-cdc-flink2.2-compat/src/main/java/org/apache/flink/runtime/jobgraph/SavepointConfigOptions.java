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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.Documentation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.core.execution.RestoreMode;

/**
 * Copy from <a
 * href="https://github.com/apache/flink/blob/release-1.20.3/flink-runtime/src/main/java/org/apache/flink/runtime/jobgraph/SavepointConfigOptions.java">...</a>.
 */
@Deprecated
@Documentation.ExcludeFromDocumentation("Hidden for deprecated.")
@PublicEvolving
public class SavepointConfigOptions {
    public static final ConfigOption<String> SAVEPOINT_PATH =
            ConfigOptions.key("execution.savepoint.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Path to a savepoint to restore the job from (for example hdfs:///flink/savepoint-1537).");
    public static final ConfigOption<Boolean> SAVEPOINT_IGNORE_UNCLAIMED_STATE =
            ConfigOptions.key("execution.savepoint.ignore-unclaimed-state")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Allow to skip savepoint state that cannot be restored. Allow this if you removed an operator from your pipeline after the savepoint was triggered.");
    public static final ConfigOption<RestoreMode> RESTORE_MODE;

    static {
        RESTORE_MODE =
                ConfigOptions.key("execution.savepoint-restore-mode")
                        .enumType(RestoreMode.class)
                        .defaultValue(RestoreMode.DEFAULT)
                        .withDescription(
                                "Describes the mode how Flink should restore from the given savepoint or retained checkpoint.");
    }
}
