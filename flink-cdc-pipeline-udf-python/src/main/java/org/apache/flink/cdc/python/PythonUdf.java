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

package org.apache.flink.cdc.python;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.ConfigOptions;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;
import org.apache.flink.cdc.common.udf.UserDefinedFunctionContext;
import org.apache.flink.cdc.python.utils.PythonUdfSignature;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/** Generic UDF that delegates to a Python function defined inline in YAML. */
@Experimental
public final class PythonUdf implements UserDefinedFunction {

    public static final ConfigOption<String> OPTION_SOURCE =
            ConfigOptions.key("source")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inline Python source containing a `def eval(...)`.");

    public static final ConfigOption<String> OPTION_PYTHON_EXECUTABLE =
            ConfigOptions.key("python-executable")
                    .stringType()
                    .defaultValue("python3")
                    .withDescription(
                            "Path to the Python interpreter Pemja embeds on every TaskManager."
                                    + " The interpreter must have a matching `pemja` package"
                                    + " installed; defaults to the first `python3` on PATH.");

    private static final String PYTHON_FUNCTION_NAME = "eval";

    private transient PythonInterpreter interpreter;

    @Override
    public void open(UserDefinedFunctionContext context) {
        Configuration config = context.configuration();
        String source = requireSource(config);
        String pythonExec = config.get(OPTION_PYTHON_EXECUTABLE);

        PythonInterpreterConfig pemjaConfig =
                PythonInterpreterConfig.newBuilder().setPythonExec(pythonExec).build();
        this.interpreter = new PythonInterpreter(pemjaConfig);
        this.interpreter.exec(source);
    }

    @Override
    public void close() {
        if (interpreter != null) {
            try {
                interpreter.close();
            } finally {
                interpreter = null;
            }
        }
    }

    @Override
    public DataType getReturnType(UserDefinedFunctionContext context) {
        Configuration config = context.configuration();
        String source = requireSource(config);
        return PythonUdfSignature.parseReturnType(source, config.get(OPTION_PYTHON_EXECUTABLE));
    }

    public Object eval(Object... args) {
        if (interpreter == null) {
            throw new IllegalStateException("PythonUdf invoked before open() was called.");
        }
        return interpreter.invoke(PYTHON_FUNCTION_NAME, args);
    }

    private static String requireSource(Configuration config) {
        return config.getOptional(OPTION_SOURCE)
                .orElseThrow(
                        () ->
                                new IllegalArgumentException(
                                        "Python UDF is missing required option '"
                                                + OPTION_SOURCE.key()
                                                + "'."));
    }
}
