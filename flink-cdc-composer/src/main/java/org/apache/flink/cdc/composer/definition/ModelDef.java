package org.apache.flink.cdc.composer.definition;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.Objects;

public class ModelDef {
    private final String name;
    private final String host;
    private final String apiKey;

    public ModelDef(String name, String host, String apiKey) {
        this.name = name;
        this.host = host;
        this.apiKey = apiKey;
    }

    public String getName() {
        return name;
    }

    public String getHost() {
        return host;
    }

    public String getApiKey() {
        return apiKey;
    }

    // 创建一个表示这个模型的 UDF
    public ScalarFunction createUdf() {
        return new ModelUdf(this);
    }

    // 内部类，代表这个模型的 UDF
    public class ModelUdf extends ScalarFunction {
        private final ModelDef model;

        public ModelUdf(ModelDef model) {
            this.model = model;
        }

        // UDF 的主要方法，处理输入并返回结果
        public String eval(String input) {
            // 这里实现调用模型 API 的逻辑
            // 使用 model.getHost() 和 model.getApiKey() 来访问 API
            // 这只是一个示例实现，实际逻辑需要根据具体的 API 调用方式来编写
            return "Embedding for: " + input;
        }

        @Override
        public void open(FunctionContext context) throws Exception {
            // 初始化逻辑，如建立API连接等
        }

        @Override
        public void close() throws Exception {
            // 清理逻辑，如关闭API连接等
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelDef modelDef = (ModelDef) o;
        return Objects.equals(name, modelDef.name)
                && Objects.equals(host, modelDef.host)
                && Objects.equals(apiKey, modelDef.apiKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, host, apiKey);
    }

    @Override
    public String toString() {
        return "ModelDef{"
                + "name='"
                + name
                + '\''
                + ", host='"
                + host
                + '\''
                + ", apiKey='"
                + apiKey
                + '\''
                + '}';
    }
}
