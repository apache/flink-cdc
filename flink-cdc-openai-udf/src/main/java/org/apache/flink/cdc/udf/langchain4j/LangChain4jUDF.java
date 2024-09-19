package org.apache.flink.cdc.udf.langchain4j;

import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.common.udf.UserDefinedFunction;

import dev.ai4j.openai4j.OpenAiClient;
import dev.ai4j.openai4j.chat.ChatCompletionRequest;
import dev.ai4j.openai4j.chat.ChatCompletionResponse;
import dev.langchain4j.model.chat.ChatLanguageModel;
import dev.langchain4j.model.openai.OpenAiChatModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class LangChain4jUDF implements UserDefinedFunction {

    private static final Logger LOG = LoggerFactory.getLogger(LangChain4jUDF.class);
    private static final String API_KEY = "sk-WegHEuogRpIyRSwaF5Ce6fE3E62e459dA61eFaF6CcF8C79b";
    private static final String MODEL_NAME = "gpt-3.5-turbo";
    private static final int TIMEOUT_SECONDS = 30;
    private static final String BASE_URL = "https://api.xty.app/v1/";

    private ChatLanguageModel model;

    // 无参构造器
    public LangChain4jUDF() {}

    // eval 方法的各种重载
    public String eval(String input) {
        return generateResponse(input);
    }

    public String eval(Integer input) {
        return generateResponse(input.toString());
    }

    public String eval(Double input) {
        return generateResponse(input.toString());
    }

    public String eval(Boolean input) {
        return generateResponse(input.toString());
    }

    private String generateResponse(String input) {
        if (input == null || input.trim().isEmpty()) {
            LOG.warn("Received empty or null input");
            return "";
        }

        try {
            String response = model.generate(input);
            LOG.debug("Generated response for input: {}", input);
            return response;
        } catch (Exception e) {
            LOG.error("Error generating response for input: {}", input, e);
            return "Error: Unable to generate response";
        }
    }

    @Override
    public DataType getReturnType() {
        return DataTypes.ARRAY(DataTypes.DOUBLE());
    }

    @Override
    public void open() throws Exception {
        LOG.info("Opening LangChain4jUDF");
        this.model =
                OpenAiChatModel.builder()
                        .baseUrl(BASE_URL) // 设置中转网站的基础 URL
                        .apiKey(API_KEY)
                        .modelName(MODEL_NAME)
                        .temperature(0.7)
                        .timeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                        .build();
        LOG.info(
                "Initialized LangChain4jUDF with model: {} and base URL: {}", MODEL_NAME, BASE_URL);
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing LangChain4jUDF");
        // Any cleanup code can go here
    }

    public static void main(String[] args) {
        OpenAiClient client =
                OpenAiClient.builder()
                        .openAiApiKey(API_KEY)
                        .baseUrl(BASE_URL) // 设置中转网站的基础 URL
                        .callTimeout(Duration.ofSeconds(TIMEOUT_SECONDS))
                        .build();

        ChatCompletionRequest request =
                ChatCompletionRequest.builder()
                        .model(MODEL_NAME)
                        .addUserMessage("Hello, can you hear me?")
                        .build();

        try {
            System.out.println("Sending test request to API...");
            ChatCompletionResponse response = client.chatCompletion(request).execute();
            System.out.println("Response received successfully.");
            System.out.println("AI response: " + response.content());
        } catch (Exception e) {
            System.err.println("Error occurred during API call:");
            e.printStackTrace();
        } finally {
            client.shutdown();
            System.out.println("OpenAI client shut down.");
        }
    }
}
