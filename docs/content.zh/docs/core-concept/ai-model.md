---
title: "AI 模型"
weight: 9
type: docs
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# AI 模型

AI 模型可用于 transform 表达式中的文本生成、文本分析、embedding 和图片理解。

## AI Functions

模型名称必须是字符串常量，并引用 `pipeline.model` 中声明的模型。文本、embedding 和图片函数分别要求模型客户端实现对应的 capability；Pipeline 会在执行前校验引用模型的 capability 是否匹配。

所有文本函数都会将模型返回的 JSON 解析为 `VARIANT`。

| 函数 | 说明 | JSON 字段 |
|------|------|-----------|
| `AI_COMPLETE(model, input, system_prompt)` | 使用调用方提供的 system prompt 补全文本。 | `result` |
| `AI_CLASSIFY(model, input, labels)` | 将输入分类到给定标签之一。 | `category`、`confidence` |
| `AI_TRANSLATE(model, input, source_lang, target_lang)` | 翻译输入；`source_lang` 可传 `auto` 自动识别。 | `translated_text`、`detected_language` |
| `AI_SUMMARIZE(model, input, max_length)` | 在指定字符数内生成摘要。 | `summary` |
| `AI_SENTIMENT(model, input)` | 分析文本情感。 | `score`、`label`、`confidence` |
| `AI_EXTRACT(model, input, schema)` | 按 schema 字符串描述提取字段。 | `extracted_json` |
| `AI_MASK(model, input, entities)` | 对指定实体类型进行脱敏。 | `masked_text`、`detected_entities` |
| `AI_EMBED(model, input)` | 生成 embedding 向量。 | 不返回 JSON，而是返回 `ARRAY<FLOAT>`。 |

以下多模态函数从 `BYTES` 字段读取图片数据：

| 函数 | 说明 | 返回类型 |
|------|------|----------|
| `AI_IMAGE_COMPLETE(model, image, prompt)` | 根据图片和自然语言 prompt 生成文本。 | `STRING` |
| `AI_IMAGE_EMBED(model, image)` | 将图片转换为 embedding 向量。 | `ARRAY<FLOAT>` |

OpenAI-compatible 模型客户端通过标准 vision chat 支持 `AI_IMAGE_COMPLETE`。客户端会识别 PNG、JPEG、GIF 和 WebP 图片，并将图片编码为 Base64 data URL。图片为 `NULL` 时直接返回 `NULL`，且不会调用模型；图片为空或格式无法识别时，会在发送请求前报错。

`AI_IMAGE_EMBED` 当前只提供框架函数和 provider capability。OpenAI API 目前没有定义标准的图片向量化协议，因此图片 embedding 需要由具体 provider 单独实现。OpenAI-compatible 模型客户端不实现图片 embedding，社区发行包目前也没有可用于生产的图片 embedding provider。需要图片向量化的用户需要等待后续 provider 实现。

六个专用文本函数使用内置英文 prompt 模板，但输入文本可以是任意语言。输入为 `NULL` 时直接返回 `NULL`，且不会调用模型；模型返回 `NULL` 时也返回 `NULL`。非空文本响应必须是语法合法的 JSON，否则当前记录处理失败，错误信息会标明具体 AI 函数。运行时只校验 JSON 语法，不校验响应字段是否存在或字段类型是否匹配。

## OpenAI-compatible 模型客户端

AI 模型客户端可供上述 AI Functions 引用。使用时，需要通过 `--jar` 将模型实现 JAR（例如 `flink-cdc-pipeline-model-openai-compatible`）添加到 Pipeline 命令中。

OpenAI-compatible 客户端支持调用实现 OpenAI Chat Completions、vision chat 和 Embeddings REST API 的服务。

system prompt、函数 prompt 和输入文本均支持英文或中文内容。

```yaml
transform:
  - source-table: db.\.*
    projection: >-
      *,
      AI_COMPLETE('completion_model', content, '总结输入内容') AS summary,
      AI_SENTIMENT('completion_model', content) AS sentiment,
      AI_IMAGE_COMPLETE('vision_model', image, '描述这张图片') AS image_description,
      AI_EMBED('embedding_model', content) AS embedding

pipeline:
  model:
    - name: completion_model
      type: openai-compatible
      options:
        model: gpt-4o-mini
        endpoint: https://api.example.com/v1
        api-key: <api-key>
        system-prompt: 你是一个简洁的助手。
        temperature: 0.2
        max-tokens: 256
    - name: embedding_model
      type: openai-compatible
      options:
        model: text-embedding-3-small
        endpoint: https://api.example.com/v1
        api-key: <api-key>
        dimension: 768
    - name: vision_model
      type: openai-compatible
      options:
        model: gpt-4o-mini
        endpoint: https://api.example.com/v1
        api-key: <api-key>
```

不要将 API Key 提交到代码仓库中，请通过部署环境的密钥管理机制提供。

### OpenAI-compatible 配置项

| 配置项 | 是否必填 | 说明 |
|--------|----------|------|
| `model` | 是 | 发送给服务端的模型名称；`model-name` 作为废弃别名仍可使用。 |
| `endpoint` | 是 | OpenAI-compatible 服务的 Base URL。 |
| `api-key` | 是 | 请求认证使用的 Bearer Token。 |
| `system-prompt` | 否 | 添加在所有文本 AI Function 的 prompt 之前。 |
| `user-prompt` | 否 | 在输入之后追加一条 user message。 |
| `temperature`、`top-p`、`stop`、`max-tokens` | 否 | 常用文本生成参数。 |
| `presence-penalty`、`frequency-penalty`、`n`、`seed` | 否 | 其他文本生成参数。 |
| `response-format` | 否 | 支持 `json_object`；文本 AI Function 的结果必须是合法 JSON。 |
| `content-type` | 否 | `text`（默认）或 `image_url`。 |
| `dimension` | 否 | 请求的 embedding 维度。 |
| `extra-header`、`extra-body` | 否 | JSON 对象格式的厂商自定义请求头或请求体字段。 |
| `error-handling-strategy` | 否 | `retry`（默认）、`failover` 或 `ignore`。 |
| `retry-num` | 否 | 最大尝试次数，默认 `100`。 |
| `retry-fallback-strategy` | 否 | 重试耗尽后的策略，可选 `failover`（默认）或 `ignore`。 |
| `retry-backoff-strategy` | 否 | `fixed`（默认）或 `exponential`。 |
| `retry-backoff-base-interval` | 否 | 重试基础间隔，默认 `1 s`。 |

## 旧版 Embedding AI 模型（已废弃）

> **已废弃：** 基于 `model-name` 和 `class-name` 的旧版模型 API 已废弃，并计划在未来移除。新 Pipeline 请使用上面基于 Factory 的 OpenAI-compatible 模型客户端。

旧版 Embedding AI 模型可以在 transform 规则中使用。使用时，需要下载内置模型 JAR，并在 `flink-cdc.sh` 命令中添加 `--jar {$BUILT_IN_MODEL_PATH}`。

如何定义一个 Embedding AI 模型：

```yaml
pipeline:
  model:
    - model-name: CHAT
      class-name: OpenAIChatModel
      openai.model: text-embedding-3-small
      openai.host: https://xxxx
      openai.apikey: abcd1234
      openai.chat.prompt: please summary this
    - model-name: GET_EMBEDDING
      class-name: OpenAIEmbeddingModel
      openai.model: text-embedding-3-small
      openai.host: https://xxxx
      openai.apikey: abcd1234
```

注意：

* `model-name` 是所有支持模型通用的必填参数，表示在 `projection` 或 `filter` 中调用的函数名称。
* `class-name` 是所有支持模型通用的必填参数，可用值参见[所有支持的模型](#所有支持的模型)。
* `openai.model`、`openai.host`、`openai.apikey` 和 `openai.chat.prompt` 是由具体模型定义的配置项。

如何使用一个 Embedding AI 模型：

```yaml
transform:
  - source-table: db.\.*
    projection: "*, inc(inc(inc(id))) as inc_id, GET_EMBEDDING(page) as emb, CHAT(page) as summary"
    filter: inc(id) < 100
pipeline:
  model:
    - model-name: CHAT
      class-name: OpenAIChatModel
      openai.model: gpt-4o-mini
      openai.host: http://langchain4j.dev/demo/openai/v1
      openai.apikey: demo
      openai.chat.prompt: please summary this
    - model-name: GET_EMBEDDING
      class-name: OpenAIEmbeddingModel
      openai.model: text-embedding-3-small
      openai.host: http://langchain4j.dev/demo/openai/v1
      openai.apikey: demo
```

这里，`GET_EMBEDDING` 通过 `model-name` 在 `pipeline` 中定义。

### 所有支持的模型

下面列出了所有支持的内置模型：

#### OpenAIChatModel

| 参数 | 类型 | 是否必填 | 含义 |
|------|------|----------|------|
| `openai.model` | STRING | 是 | 要调用的模型名称，例如 `gpt-4o-mini`。可用选项有 `gpt-4o-mini`、`gpt-4o`、`gpt-4-32k` 和 `gpt-3.5-turbo`。 |
| `openai.host` | STRING | 是 | 模型服务器地址，例如 `http://langchain4j.dev/demo/openai/v1`。 |
| `openai.apikey` | STRING | 是 | 模型服务器验证使用的 API Key，例如 `demo`。 |
| `openai.chat.prompt` | STRING | 否 | 与 OpenAI 聊天的提示词，例如 `Please summarize this`。 |

#### OpenAIEmbeddingModel

| 参数 | 类型 | 是否必填 | 含义 |
|------|------|----------|------|
| `openai.model` | STRING | 是 | 要调用的模型名称，例如 `text-embedding-3-small`。可用选项有 `text-embedding-3-small`、`text-embedding-3-large` 和 `text-embedding-ada-002`。 |
| `openai.host` | STRING | 是 | 模型服务器地址，例如 `http://langchain4j.dev/demo/openai/v1`。 |
| `openai.apikey` | STRING | 是 | 模型服务器验证使用的 API Key，例如 `demo`。 |
