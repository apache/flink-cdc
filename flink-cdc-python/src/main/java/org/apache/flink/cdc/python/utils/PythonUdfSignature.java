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

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;

import pemja.core.PythonInterpreter;
import pemja.core.PythonInterpreterConfig;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.stream.Collectors;

/** Resolves the return type of a {@code PythonUdf} from its inline Python source. */
public final class PythonUdfSignature {

    private static final String SIGNATURE_PARSER_RESOURCE =
            "/org/apache/flink/cdc/python/signature.py";

    private static final Map<String, DataType> SUPPORTED_ANNOTATIONS =
            Map.of(
                    "bool", DataTypes.BOOLEAN(),
                    "int", DataTypes.BIGINT(),
                    "float", DataTypes.DOUBLE(),
                    "str", DataTypes.STRING(),
                    "bytes", DataTypes.BYTES());

    private PythonUdfSignature() {}

    public static DataType parseReturnType(String source, String pythonExec) {
        String annotation = parseReturnAnnotation(source, pythonExec).trim();
        DataType type = SUPPORTED_ANNOTATIONS.get(annotation);
        if (type == null) {
            throw new IllegalArgumentException(
                    "Unsupported Python UDF return type '"
                            + annotation
                            + "'. Supported annotations: "
                            + SUPPORTED_ANNOTATIONS.keySet().stream()
                                    .sorted()
                                    .collect(Collectors.toList()));
        }
        return type;
    }

    private static String parseReturnAnnotation(String source, String pythonExec) {
        PythonInterpreterConfig pemjaConfig =
                PythonInterpreterConfig.newBuilder().setPythonExec(pythonExec).build();
        try (PythonInterpreter parser = new PythonInterpreter(pemjaConfig)) {
            parser.exec(loadSignatureScript());
            return (String) parser.invoke("eval_return_type", source);
        }
    }

    private static String loadSignatureScript() {
        try (InputStream in =
                PythonUdfSignature.class.getResourceAsStream(SIGNATURE_PARSER_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException(
                        "Bundled signature parser resource not found: "
                                + SIGNATURE_PARSER_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalArgumentException("Failed to parse Python UDF signature", e);
        }
    }
}
