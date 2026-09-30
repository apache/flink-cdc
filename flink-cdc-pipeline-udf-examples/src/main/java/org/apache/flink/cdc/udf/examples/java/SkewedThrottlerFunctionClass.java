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

/** A UDF that makes one record deliberately slower than later records. */
public class SkewedThrottlerFunctionClass implements UserDefinedFunction {

    public String eval(Object value, int slowValue, int slowSeconds) {
        if (asLong(value) == slowValue) {
            try {
                Thread.sleep(slowSeconds * 1000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted while throttling a record.", e);
            }
        }
        System.out.println("SkewedThrottlerFunctionClass finished " + value);
        return "throttled_" + value;
    }

    private long asLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }
}
