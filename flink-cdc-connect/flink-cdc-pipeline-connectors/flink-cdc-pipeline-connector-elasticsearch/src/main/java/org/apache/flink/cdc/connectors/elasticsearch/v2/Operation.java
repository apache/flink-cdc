/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.cdc.connectors.elasticsearch.v2;

import co.elastic.clients.elasticsearch.core.bulk.BulkOperationVariant;

import java.io.Serializable;
import java.util.Objects;

/**
 * A single stream element which contains a {@link BulkOperationVariant}. This class represents an
 * operation that will be performed in an Elasticsearch bulk request.
 */
public class Operation implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BulkOperationVariant bulkOperationVariant;

    /**
     * Constructs an Operation with the specified {@link BulkOperationVariant}.
     *
     * @param bulkOperation the BulkOperationVariant to be wrapped by this Operation.
     */
    public Operation(BulkOperationVariant bulkOperation) {
        this.bulkOperationVariant = bulkOperation;
    }

    /**
     * Returns the {@link BulkOperationVariant} contained in this Operation.
     *
     * @return the BulkOperationVariant instance.
     */
    public BulkOperationVariant getBulkOperationVariant() {
        return bulkOperationVariant;
    }

    @Override
    public int hashCode() {
        return Objects.hash(bulkOperationVariant);
    }

    @Override
    public String toString() {
        return "Operation{" + "bulkOperationVariant=" + bulkOperationVariant + '}';
    }
}
