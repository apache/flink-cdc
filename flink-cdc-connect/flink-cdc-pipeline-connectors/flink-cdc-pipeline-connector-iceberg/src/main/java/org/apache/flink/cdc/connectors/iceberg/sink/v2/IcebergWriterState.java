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

package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import java.util.Objects;

/** The state of the {@link IcebergWriter}. */
public class IcebergWriterState {

    // The job ID associated with this writer state
    private final String jobId;

    // The operator ID associated with this writer state
    private final String operatorId;

    public IcebergWriterState(String jobId, String operatorId) {
        this.jobId = jobId;
        this.operatorId = operatorId;
    }

    public String getJobId() {
        return jobId;
    }

    public String getOperatorId() {
        return operatorId;
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, operatorId);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        IcebergWriterState that = (IcebergWriterState) obj;
        return Objects.equals(jobId, that.jobId) && Objects.equals(operatorId, that.operatorId);
    }

    @Override
    public String toString() {
        return "IcebergWriterState{"
                + "jobId='"
                + jobId
                + '\''
                + ", operatorId='"
                + operatorId
                + '\''
                + '}';
    }
}
