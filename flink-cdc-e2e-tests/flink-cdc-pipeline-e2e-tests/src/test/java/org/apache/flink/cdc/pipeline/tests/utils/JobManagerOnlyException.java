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

package org.apache.flink.cdc.pipeline.tests.utils;

/**
 * A {@link RuntimeException} that is deliberately packaged into a jar which is only installed into
 * the JobManager container's {@code /opt/flink/lib}, and never shipped to the TaskManager container
 * or submitted with the pipeline jars.
 *
 * <p>It emulates the classpath asymmetry seen in production (e.g. a JDBC driver present in the
 * JobManager's lib directory but absent from the TaskManager), so that a coordinator-side failure
 * of this class cannot be deserialized on the TaskManager unless it crosses the
 * operator-coordinator RPC boundary as a {@code SerializedThrowable} (FLINK-40040).
 */
public class JobManagerOnlyException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JobManagerOnlyException(String message) {
        super(message);
    }
}
