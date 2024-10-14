package org.apache.flink.cdc.runtime.operators.model;

import org.apache.flink.cdc.common.data.ArrayData;
import org.apache.flink.cdc.common.data.GenericArrayData;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;

import com.google.gson.Gson;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.openai.OpenAiEmbeddingModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ModelUdf implements UserDefinedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(ModelUdf.class);
    private static final String DEFAULT_API_KEY =
            "sk-WegHEuogRpIyRSwaF5Ce6fE3E62e459dA61eFaF6CcF8C79b";
    private static final String DEFAULT_MODEL_NAME = "text-embedding-ada-002";
    private static final int DEFAULT_TIMEOUT_SECONDS = 30;
    private static final String DEFAULT_BASE_URL = "https://api.gpt.ge/v1/";

    private String name;
    private String host;
    private String apiKey;
    private String modelName;
    private int timeoutSeconds;
    private OpenAiEmbeddingModel embeddingModel;

    public ModelUdf() {
        // Default constructor
    }

    public void configure(String serializedParams) {
        Map<String, String> params = new Gson().fromJson(serializedParams, Map.class);
        this.name = params.get("name");
        this.host = params.getOrDefault("host", DEFAULT_BASE_URL);
        this.apiKey = params.getOrDefault("apiKey", DEFAULT_API_KEY);
        this.modelName = params.getOrDefault("modelName", DEFAULT_MODEL_NAME);
        this.timeoutSeconds =
                Integer.parseInt(
                        params.getOrDefault(
                                "timeoutSeconds", String.valueOf(DEFAULT_TIMEOUT_SECONDS)));
        LOG.info("Configured ModelUdf: {} with host: {}", name, host);
    }

    public ArrayData eval(String input) {
        return getEmbedding(input);
    }

    public ArrayData eval(Integer input) {
        return getEmbedding(input.toString());
    }

    public ArrayData eval(Double input) {
        return getEmbedding(input.toString());
    }

    public ArrayData eval(Boolean input) {
        return getEmbedding(input.toString());
    }

    // Method to support multiple parameters
    public ArrayData eval(Object... inputs) {
        String combinedInput =
                Arrays.stream(inputs).map(Object::toString).collect(Collectors.joining(" "));
        return getEmbedding(combinedInput);
    }

    private ArrayData getEmbedding(String input) {
        if (input == null || input.trim().isEmpty()) {
            LOG.debug("Empty or null input provided for embedding.");
            return new GenericArrayData(new Float[0]); // 返回空的 ArrayData
        }

        try {
            // 创建 TextSegment 对象
            TextSegment textSegment = new TextSegment(input, new Metadata());

            // 获取嵌入结果
            List<Embedding> embeddings = embeddingModel.embedAll(Collections.singletonList(textSegment)).content();

            if (embeddings != null && !embeddings.isEmpty()) {
                List<Float> embeddingList = embeddings.get(0).vectorAsList();
                // 将 List<Float> 转换为 ArrayData
                Float[] embeddingArray = embeddingList.toArray(new Float[0]);
                return new GenericArrayData(embeddingArray);
            } else {
                LOG.debug("No embedding results returned for input: {}", input);
                return new GenericArrayData(new Float[0]);
            }
        } catch (Exception e) {
            LOG.error("Error while getting embedding for input: {}", input, e);
            return new GenericArrayData(new Float[0]);
        }
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.ARRAY(DataTypes.FLOAT());
    }

    @Override
    public void open() throws Exception {
        LOG.info("Opening ModelUdf: {}", name);
        this.embeddingModel =
                OpenAiEmbeddingModel.builder()
                        .apiKey(apiKey)
                        .baseUrl(host)
                        .modelName(modelName)
                        .timeout(Duration.ofSeconds(timeoutSeconds))
                        .maxRetries(3)
                        .build();
        LOG.info("Initialized ModelUdf: {} with host: {}", name, host);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing ModelUdf: {}", name);
        // Any cleanup code can go here
    }
}
