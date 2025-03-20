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

package org.apache.flink.cdc.connectors.elasticsearch.serializer;

import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.Requests;

import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * A utility class that creates Elasticsearch 6.x specific requests.
 *
 * <p>This class provides methods to create {@link IndexRequest} and {@link DeleteRequest} objects
 * that are compatible with Elasticsearch 6.x based on the operations provided.
 */
public class Elasticsearch6RequestCreator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * Creates an Elasticsearch 6 IndexRequest.
     *
     * @param operation IndexOperation object.
     * @return IndexRequest object.
     */
    public static IndexRequest createIndexRequest(IndexOperation<?> operation) {
        // Convert the document to Map<String, Object>
        Map<String, Object> documentMap =
                objectMapper.convertValue(operation.document(), Map.class);

        // Create and return IndexRequest, ensuring type field is set
        return Requests.indexRequest()
                .index(operation.index())
                .type("_doc") // Assuming type is "_doc", adjust as necessary
                .id(operation.id())
                .source(documentMap);
    }

    /**
     * Creates an Elasticsearch 6 DeleteRequest.
     *
     * @param operation DeleteOperation object.
     * @return DeleteRequest object.
     */
    public static DeleteRequest createDeleteRequest(DeleteOperation operation) {
        String index = operation.index();
        String id = operation.id();

        // Create and return DeleteRequest, ensuring type field is set
        return Requests.deleteRequest(index)
                .type("_doc") // Assuming type is "_doc", adjust as necessary
                .id(id);
    }
}
