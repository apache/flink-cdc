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

package org.apache.flink.cdc.connectors.lancedb.serde;

import org.apache.flink.cdc.common.event.OperationType;

import java.util.Collections;
import java.util.List;

/** Converted Lance row operation. */
public class LanceDbOperation {

    private final String datasetPath;
    private final OperationType operationType;
    private final List<Object> values;

    public LanceDbOperation(String datasetPath, OperationType operationType, List<Object> values) {
        this.datasetPath = datasetPath;
        this.operationType = operationType;
        this.values = Collections.unmodifiableList(values);
    }

    public String getDatasetPath() {
        return datasetPath;
    }

    public OperationType getOperationType() {
        return operationType;
    }

    public List<Object> getValues() {
        return values;
    }
}
