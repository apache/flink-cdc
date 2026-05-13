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

package org.apache.flink.cdc.python.utils;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

/**
 * Fails fast with an actionable message if Pemja or its Python sidecar isn't reachable. The
 * interpreter is resolved from {@code pemja.python.executable} (system property) or {@code
 * PEMJA_PYTHON_EXECUTABLE} (env var), falling back to {@code python3} on PATH.
 */
public final class PemjaTestSupport {

    public static final String PYTHON_EXEC =
            System.getProperty(
                    "pemja.python.executable",
                    System.getenv().getOrDefault("PEMJA_PYTHON_EXECUTABLE", "python3"));

    private PemjaTestSupport() {}

    public static void requirePemja() {
        try (PythonInterpreter ignored =
                new PythonInterpreter(
                        PythonInterpreterConfig.newBuilder().setPythonExec(PYTHON_EXEC).build())) {
            // Smoke-test: starting the interpreter loads libpython + the pemja package.
        } catch (Throwable t) {
            throw new IllegalStateException(
                    "Pemja could not be initialized using '"
                            + PYTHON_EXEC
                            + "'. Install Python 3.9+ and `pip install pemja==0.5.5`, or point"
                            + " the tests at a usable interpreter via"
                            + " -Dpemja.python.executable=/path/to/python3"
                            + " (or PEMJA_PYTHON_EXECUTABLE env var).",
                    t);
        }
    }
}
