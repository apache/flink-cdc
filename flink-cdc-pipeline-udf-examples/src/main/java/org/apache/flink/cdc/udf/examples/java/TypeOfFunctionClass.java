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

import org.apache.flink.cdc.common.udf.UserDefinedFunction;

/** This is an example UDF class for testing purposes only. */
public class TypeOfFunctionClass implements UserDefinedFunction {
    public String eval(Boolean b) {
        return "Boolean: " + b;
    }

    public String eval(Integer i) {
        return "Integer: " + i;
    }

    public String eval(Float f) {
        return "Float: " + f;
    }

    public String eval(Double d) {
        return "Double: " + d;
    }

    public String eval(String s) {
        return "String: " + s;
    }
}
