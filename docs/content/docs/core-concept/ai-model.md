---
title: "AI Model"
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

# AI Model

AI models can be used in transform expressions for text generation, text analysis, embedding, and
image understanding.

## AI Functions

The model name must be a string constant that refers to a model declared in `pipeline.model`. Text functions require a model client that implements text generation, while embedding and image functions require their corresponding capabilities. The pipeline validates the referenced model capability before execution.

All text functions return `VARIANT` values parsed from the model's JSON response.

| Function | Description | JSON fields |
|----------|-------------|-------------|
| `AI_COMPLETE(model, input, system_prompt)` | Completes the input using a caller-provided system prompt. | `result` |
| `AI_CLASSIFY(model, input, labels)` | Classifies the input into one of the provided labels. | `category`, `confidence` |
| `AI_TRANSLATE(model, input, source_lang, target_lang)` | Translates the input. Use `auto` to detect the source language. | `translated_text`, `detected_language` |
| `AI_SUMMARIZE(model, input, max_length)` | Summarizes the input within the requested character limit. | `summary` |
| `AI_SENTIMENT(model, input)` | Analyzes sentiment. | `score`, `label`, `confidence` |
| `AI_EXTRACT(model, input, schema)` | Extracts fields described by the schema string. | `extracted_json` |
| `AI_MASK(model, input, entities)` | Masks the requested entity types. | `masked_text`, `detected_entities` |
| `AI_EMBED(model, input)` | Creates an embedding vector. | Returns `ARRAY<FLOAT>` instead of JSON. |

The following multimodal functions accept image data from a `BYTES` column:

| Function | Description | Return type |
|----------|-------------|-------------|
| `AI_IMAGE_COMPLETE(model, image, prompt)` | Generates text from an image and a natural-language prompt. | `STRING` |
| `AI_IMAGE_EMBED(model, image)` | Converts an image into an embedding vector. | `ARRAY<FLOAT>` |

The OpenAI-compatible model client supports `AI_IMAGE_COMPLETE` through standard vision chat. It
detects PNG, JPEG, GIF, and WebP images and sends the image as a Base64 data URL. A `NULL` image
returns `NULL` without invoking the model, while empty or unrecognized image data is rejected before
the request is sent.

`AI_IMAGE_EMBED` currently provides only the framework function and provider capability. The OpenAI
API does not define a standard image embedding protocol, so image embedding requires a
provider-specific implementation. The OpenAI-compatible model client does not implement image
embedding, and the community distribution does not yet include a production provider for it. Users
who need image embedding must wait for a follow-up provider implementation.

The specialized text functions use built-in English prompt templates, but their input may be in any language. If the input is `NULL`, the function returns `NULL` without invoking the model. A `NULL` model response also produces `NULL`. A non-null text response must be syntactically valid JSON; otherwise, record processing fails with an error that identifies the AI function. The runtime validates JSON syntax but does not validate the presence or types of individual response fields.

## OpenAI-compatible Model Client

AI model clients can be referenced by the AI functions above. Add the model implementation JAR, such as `flink-cdc-pipeline-model-openai-compatible`, to the pipeline command with `--jar`.

The OpenAI-compatible client supports chat completions, vision chat, and text embeddings against endpoints that implement the corresponding OpenAI REST APIs.

System prompts, function prompts, and input text may contain either English or Chinese content.

```yaml
transform:
  - source-table: db.\.*
    projection: >-
      *,
      AI_COMPLETE('completion_model', content, 'Summarize the input') AS summary,
      AI_SENTIMENT('completion_model', content) AS sentiment,
      AI_IMAGE_COMPLETE('vision_model', image, 'Describe the image') AS image_description,
      AI_EMBED('embedding_model', content) AS embedding

pipeline:
  model:
    - name: completion_model
      type: openai-compatible
      options:
        model: gpt-4o-mini
        endpoint: https://api.example.com/v1
        api-key: <api-key>
        system-prompt: You are a concise assistant.
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

Do not store API keys in source control. Supply them through the secret-management mechanism of your deployment environment.

### OpenAI-compatible Options

| Option | Required | Description |
|--------|----------|-------------|
| `model` | Yes | Model name sent to the endpoint. `model-name` is accepted as a deprecated alias. |
| `endpoint` | Yes | Base URL of the OpenAI-compatible endpoint. |
| `api-key` | Yes | Bearer token used to authenticate requests. |
| `system-prompt` | No | Prompt prepended to every text AI function prompt. |
| `user-prompt` | No | Additional user message appended after the input. |
| `temperature`, `top-p`, `stop`, `max-tokens` | No | Common generation parameters. |
| `presence-penalty`, `frequency-penalty`, `n`, `seed` | No | Additional generation parameters. |
| `response-format` | No | `json_object` is supported. Text AI function results must be valid JSON. |
| `content-type` | No | `text` (default) or `image_url`. |
| `dimension` | No | Requested embedding dimension. |
| `extra-header`, `extra-body` | No | Provider-specific headers or body fields encoded as JSON objects. |
| `error-handling-strategy` | No | `retry` (default), `failover`, or `ignore`. |
| `retry-num` | No | Maximum number of attempts. Defaults to `100`. |
| `retry-fallback-strategy` | No | `failover` (default) or `ignore` after retries are exhausted. |
| `retry-backoff-strategy` | No | `fixed` (default) or `exponential`. |
| `retry-backoff-base-interval` | No | Base retry interval. Defaults to `1 s`. |

## Legacy Embedding AI Model (Deprecated)

> **Deprecated:** The legacy model API based on `model-name` and `class-name` is deprecated and planned for removal. Use the factory-based OpenAI-compatible model client above for new pipelines.

The legacy Embedding AI Model can be used in transform rules. To use it, download the built-in model JAR and add `--jar {$BUILT_IN_MODEL_PATH}` to your `flink-cdc.sh` command.

How to define an Embedding AI Model:

```yaml
pipeline:
  model:
    - model-name: CHAT
      class-name: OpenAIChatModel
      openai.model: gpt-4o-mini
      openai.host: https://xxxx
      openai.apikey: abcd1234
      openai.chat.prompt: please summary this
    - model-name: GET_EMBEDDING
      class-name: OpenAIEmbeddingModel
      openai.model: text-embedding-3-small
      openai.host: https://xxxx
      openai.apikey: abcd1234
```

Note:

* `model-name` is a common required parameter for all supported models. It represents the function name called in `projection` or `filter`.
* `class-name` is a common required parameter for all supported models. Available values can be found in [All Supported Models](#all-supported-models).
* `openai.model`, `openai.host`, `openai.apikey`, and `openai.chat.prompt` are options defined by a specific model.

How to use an Embedding AI Model:

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

Here, `GET_EMBEDDING` is defined through `model-name` in `pipeline`.

### All Supported Models

The following built-in models are provided:

#### OpenAIChatModel

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `openai.model` | STRING | Yes | Name of the model to call, for example, `gpt-4o-mini`. Available options are `gpt-4o-mini`, `gpt-4o`, `gpt-4-32k`, and `gpt-3.5-turbo`. |
| `openai.host` | STRING | Yes | Model server address, for example, `http://langchain4j.dev/demo/openai/v1`. |
| `openai.apikey` | STRING | Yes | API key for authenticating with the model server, for example, `demo`. |
| `openai.chat.prompt` | STRING | No | Prompt for chatting with OpenAI, for example, `Please summarize this`. |

#### OpenAIEmbeddingModel

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `openai.model` | STRING | Yes | Name of the model to call, for example, `text-embedding-3-small`. Available options are `text-embedding-3-small`, `text-embedding-3-large`, and `text-embedding-ada-002`. |
| `openai.host` | STRING | Yes | Model server address, for example, `http://langchain4j.dev/demo/openai/v1`. |
| `openai.apikey` | STRING | Yes | API key for authenticating with the model server, for example, `demo`. |
