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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/** Descriptor of a UDF function. */
@Internal
public class UserDefinedFunctionDescriptor implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String name;
    private final String classpath;
    private final String className;
    private final DataType returnTypeHint;
    private final boolean isCdcPipelineUdf;
    private final Map<String, String> parameters;

    public UserDefinedFunctionDescriptor(String name, String classpath) {
        this(name, classpath, new HashMap<>());
    }

    public UserDefinedFunctionDescriptor(
            Tuple3<String, String, Map<String, String>> descriptorTuple) {
        this(descriptorTuple.f0, descriptorTuple.f1, descriptorTuple.f2);
    }

    public UserDefinedFunctionDescriptor(
            String name, String classpath, Map<String, String> parameters) {
        this.name = name;
        this.parameters = parameters;
        this.classpath = classpath;
        this.className = classpath.substring(classpath.lastIndexOf('.') + 1);
        try {
            Class<?> clazz = Class.forName(classpath);
            isCdcPipelineUdf = isCdcPipelineUdf(clazz);
            if (isCdcPipelineUdf) {
                // We use reflection to invoke UDF methods since we may add more methods
                // into UserDefinedFunction interface, thus the provided UDF classes
                // might not be compatible with the interface definition in CDC common.
                returnTypeHint =
                        (DataType)
                                clazz.getMethod("getReturnType")
                                        .invoke(clazz.getConstructor().newInstance());
            } else {
                returnTypeHint = null;
            }
        } catch (ClassNotFoundException
                | InvocationTargetException
                | IllegalAccessException
                | NoSuchMethodException
                | InstantiationException e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate UDF " + name + "@" + classpath, e);
        }
    }

    private boolean isCdcPipelineUdf(Class<?> clazz) {
        Class<?> cdcPipelineUdfClazz = UserDefinedFunction.class;
        Class<?> flinkScalarFunctionClazz = org.apache.flink.table.functions.ScalarFunction.class;

        if (Arrays.stream(clazz.getInterfaces())
                .map(Class::getName)
                .collect(Collectors.toList())
                .contains(cdcPipelineUdfClazz.getName())) {
            return true;
        } else if (clazz.getSuperclass().getName().equals(flinkScalarFunctionClazz.getName())) {
            return false;
        } else {
            throw new IllegalArgumentException(
                    String.format(
                            "Failed to detect UDF class "
                                    + clazz
                                    + " since it never implements %s or extends Flink %s.",
                            cdcPipelineUdfClazz,
                            flinkScalarFunctionClazz));
        }
    }

    public DataType getReturnTypeHint() {
        return returnTypeHint;
    }

    public boolean isCdcPipelineUdf() {
        return isCdcPipelineUdf;
    }

    public String getName() {
        return name;
    }

    public String getClasspath() {
        return classpath;
    }

    public String getClassName() {
        return className;
    }

    public Map<String, String> getParameters() {
        return parameters;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        UserDefinedFunctionDescriptor that = (UserDefinedFunctionDescriptor) o;
        return isCdcPipelineUdf == that.isCdcPipelineUdf
                && Objects.equals(name, that.name)
                && Objects.equals(classpath, that.classpath)
                && Objects.equals(className, that.className)
                && Objects.equals(returnTypeHint, that.returnTypeHint)
                && Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                name, classpath, className, returnTypeHint, isCdcPipelineUdf, parameters);
    }

    @Override
    public String toString() {
        return "UserDefinedFunctionDescriptor{"
                + "name='"
                + name
                + '\''
                + ", classpath='"
                + classpath
                + '\''
                + ", className='"
                + className
                + '\''
                + ", returnTypeHint="
                + returnTypeHint
                + ", isCdcPipelineUdf="
                + isCdcPipelineUdf
                + ", parameters="
                + parameters
                + '}';
    }
}
