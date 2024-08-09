package org.apache.flink.cdc.connectors.elasticsearch.serializer;

import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.Requests;

import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class Elasticsearch7RequestCreator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static IndexRequest createIndexRequest(IndexOperation<?> operation) {
        Map<String, Object> documentMap =
                objectMapper.convertValue(operation.document(), Map.class);

        return Requests.indexRequest()
                .index(operation.index())
                .id(operation.id())
                .source(documentMap);
    }

    public static DeleteRequest createDeleteRequest(DeleteOperation operation) {
        String index = operation.index();
        String id = operation.id();

        return Requests.deleteRequest(index).id(id);
    }
}
