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

package org.apache.flink.cdc.connectors.mysql.source.utils;

import org.apache.flink.api.connector.source.mocks.MockSplitEnumeratorContext;
import org.apache.flink.cdc.connectors.mysql.source.split.MySqlSplit;

/** A mock enumerator context to record isProcessingBacklog. */
public class MockMySqlSplitEnumeratorEnumeratorContext
        extends MockSplitEnumeratorContext<MySqlSplit> {
    private boolean isProcessingBacklog = false;

    public MockMySqlSplitEnumeratorEnumeratorContext(int parallelism) {
        super(parallelism);
    }

    @Override
    public void setIsProcessingBacklog(boolean isProcessingBacklog) {
        this.isProcessingBacklog = isProcessingBacklog;
    }

    public boolean isProcessingBacklog() {
        return isProcessingBacklog;
    }
}
