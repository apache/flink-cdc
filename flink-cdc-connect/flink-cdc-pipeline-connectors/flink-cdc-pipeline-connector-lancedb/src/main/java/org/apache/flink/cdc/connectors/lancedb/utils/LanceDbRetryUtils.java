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

package org.apache.flink.cdc.connectors.lancedb.utils;

import java.util.Locale;

/** Retry helpers for Lance failures. */
public class LanceDbRetryUtils {

    private LanceDbRetryUtils() {}

    public static boolean isRetryable(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase(Locale.ROOT);
                if (lower.contains("timeout")
                        || lower.contains("timed out")
                        || lower.contains("temporar")
                        || lower.contains("network")
                        || lower.contains("connection")
                        || lower.contains("broken pipe")
                        || lower.contains("connection reset")
                        || lower.contains("conflict")
                        || lower.contains("commit failed")
                        || lower.contains("object store")
                        || lower.contains("rate limit")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }

    public static boolean isDatasetNotFound(Throwable failure) {
        Throwable current = failure;
        while (current != null) {
            String message = current.getMessage();
            if (message != null) {
                String lower = message.toLowerCase(Locale.ROOT);
                if (lower.contains("not found")
                        || lower.contains("does not exist")
                        || lower.contains("no such file")) {
                    return true;
                }
            }
            current = current.getCause();
        }
        return false;
    }
}
