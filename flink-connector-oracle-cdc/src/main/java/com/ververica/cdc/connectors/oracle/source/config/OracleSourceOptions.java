/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oracle.source.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import com.ververica.cdc.connectors.base.options.JdbcSourceOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;

/** Configurations for {@link OracleSource}. */
public class OracleSourceOptions extends JdbcSourceOptions {

    public static final ConfigOption<String> URL =
            ConfigOptions.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Url to use when connecting to the Oracle database server.");

    public static final ConfigOption<String> SCHEMA_NAME =
            ConfigOptions.key("schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Schema name of the Oracle database to monitor.");

    public static final ConfigOption<Integer> PORT =
            ConfigOptions.key("port")
                    .intType()
                    .defaultValue(1521)
                    .withDescription("Integer port number of the Oracle database server.");
}
