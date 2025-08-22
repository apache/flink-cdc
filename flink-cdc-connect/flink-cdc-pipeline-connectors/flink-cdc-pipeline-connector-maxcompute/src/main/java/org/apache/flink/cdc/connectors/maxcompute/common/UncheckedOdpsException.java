/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.common;

import com.aliyun.odps.OdpsException;

/** a wrapper class for {@link OdpsException} which throws {@link RuntimeException}. */
public class UncheckedOdpsException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    private final OdpsException cause;

    public UncheckedOdpsException(OdpsException cause) {
        super(cause);
        this.cause = cause;
    }

    @Override
    public OdpsException getCause() {
        return cause;
    }

    @Override
    public String getMessage() {
        return cause.getMessage() + ", requestId: " + cause.getRequestId();
    }
}
