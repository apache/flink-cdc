package org.apache.flink.cdc.connectors.elasticsearch.serializer;

import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.delete.DeleteRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch6.shaded.org.elasticsearch.client.Requests;

import co.elastic.clients.elasticsearch.core.bulk.DeleteOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class Elasticsearch6RequestCreator {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 创建一个 Elasticsearch 6 的 IndexRequest
     *
     * @param operation IndexOperation 对象
     * @return IndexRequest 对象
     */
    public static IndexRequest createIndexRequest(IndexOperation<?> operation) {
        // 将 document 转换成 Map<String, Object>
        Map<String, Object> documentMap =
                objectMapper.convertValue(operation.document(), Map.class);

        // 创建并返回 IndexRequest，确保设置 type 字段
        return Requests.indexRequest()
                .index(operation.index())
                .type("_doc") // 这里假设 type 是 "_doc"，请根据你的实际情况调整
                .id(operation.id())
                .source(documentMap);
    }

    /**
     * 创建一个 Elasticsearch 6 的 DeleteRequest
     *
     * @param operation DeleteOperation 对象
     * @return DeleteRequest 对象
     */
    public static DeleteRequest createDeleteRequest(DeleteOperation operation) {
        String index = operation.index();
        String id = operation.id();

        // 创建并返回 DeleteRequest，确保设置 type 字段
        return Requests.deleteRequest(index)
                .type("_doc") // 这里假设 type 是 "_doc"，请根据你的实际情况调整
                .id(id);
    }
}
