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

package org.apache.flink.cdc.udf.examples.java;

import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;

/** This is an example UDF class that reads options from configuration. */
public class ConfigurableFunctionClass implements UserDefinedFunction {

    private static final ConfigOption<String> GREETING =
            ConfigOptions.key("greeting").stringType().defaultValue("Hello");

    private static final ConfigOption<String> SUFFIX =
            ConfigOptions.key("suffix").stringType().defaultValue("!");

    private String greeting;
    private String suffix;

    public String eval(String value) {
        return greeting + " " + value + suffix;
    }

    @Override
    public void open(UserDefinedFunctionContext context) throws Exception {
        greeting = context.configuration().get(GREETING);
        suffix = context.configuration().get(SUFFIX);
    }

    @Override
    public void close() throws Exception {}
}
