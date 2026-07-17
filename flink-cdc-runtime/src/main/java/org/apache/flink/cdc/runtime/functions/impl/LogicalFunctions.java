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

package org.apache.flink.cdc.runtime.functions.impl;

/** Logical built-in functions. */
public class LogicalFunctions {

    public static Boolean and(Boolean left, Boolean right) {
        if (Boolean.FALSE.equals(left) || Boolean.FALSE.equals(right)) {
            return false;
        }
        if (left == null || right == null) {
            return null;
        }
        return true;
    }

    public static Boolean or(Boolean left, Boolean right) {
        if (Boolean.TRUE.equals(left) || Boolean.TRUE.equals(right)) {
            return true;
        }
        if (left == null || right == null) {
            return null;
        }
        return false;
    }

    public static Boolean not(Boolean value) {
        return value == null ? null : !value;
    }

    public static boolean isNull(Object value) {
        return value == null;
    }

    public static boolean isNotNull(Object value) {
        return value != null;
    }

    public static boolean isTrue(Boolean value) {
        return Boolean.TRUE.equals(value);
    }

    public static boolean isNotTrue(Boolean value) {
        return !Boolean.TRUE.equals(value);
    }

    public static boolean isFalse(Boolean value) {
        return Boolean.FALSE.equals(value);
    }

    public static boolean isNotFalse(Boolean value) {
        return !Boolean.FALSE.equals(value);
    }

    public static boolean isUnknown(Boolean value) {
        return value == null;
    }

    public static boolean isNotUnknown(Boolean value) {
        return value != null;
    }

    public static Object coalesce(Object... objects) {
        for (Object item : objects) {
            if (item != null) {
                return item;
            }
        }
        return null;
    }
}
