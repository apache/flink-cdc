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

package org.apache.flink.cdc.connectors.fluss.sink;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;

public class FlussDataSinkOptions {

    // prefix for passing properties for fluss options.
    public static final String PREFIX_FLUSS_PROPERTIES = "fluss.properties.";

    public static final ConfigOption<String> BOOTSTRAP_SERVERS =
            ConfigOptions.key("bootstrap.servers")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The bootstrap servers for the Fluss sink connection.");

    public static final ConfigOption<String> CLIENT_ID =
            ConfigOptions.key("client.id")
                    .stringType()
                    .defaultValue("")
                    .withDescription("An id string to pass to the server when making requests.");
}
