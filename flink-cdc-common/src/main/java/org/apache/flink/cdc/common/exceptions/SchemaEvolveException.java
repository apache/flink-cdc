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

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nullable;

/** An exception occurred during schema evolution. */
public class SchemaEvolveException extends FlinkRuntimeException {
    protected final SchemaChangeEvent applyingEvent;
    protected final String exceptionMessage;
    protected final @Nullable Throwable cause;

    public SchemaEvolveException(SchemaChangeEvent applyingEvent, String exceptionMessage) {
        this(applyingEvent, exceptionMessage, null);
    }

    public SchemaEvolveException(
            SchemaChangeEvent applyingEvent, String exceptionMessage, @Nullable Throwable cause) {
        super(cause);
        this.applyingEvent = applyingEvent;
        this.exceptionMessage = exceptionMessage;
        this.cause = cause;
    }

    public SchemaChangeEvent getApplyingEvent() {
        return applyingEvent;
    }

    public String getExceptionMessage() {
        return exceptionMessage;
    }

    @Nullable
    public Throwable getCause() {
        return cause;
    }

    @Override
    public String toString() {
        return "SchemaEvolveException{"
                + "applyingEvent="
                + applyingEvent
                + ", exceptionMessage='"
                + exceptionMessage
                + '\''
                + ", cause='"
                + cause
                + '\''
                + '}';
    }
}
