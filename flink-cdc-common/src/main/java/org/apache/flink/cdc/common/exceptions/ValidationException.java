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

package org.apache.flink.cdc.common.exceptions;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

import javax.annotation.Nullable;

/**
 * Exception for validation errors (e.g. invalid options, unsupported options). Defined in
 * flink-cdc-common to avoid runtime dependency on Flink table API (e.g. {@code
 * org.apache.flink.table.api.ValidationException}), which may not be on the classpath when running
 * with different Flink versions.
 */
@PublicEvolving
public class ValidationException extends RuntimeException {

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(String message, @Nullable Throwable cause) {
        super(message, cause);
    }
}
