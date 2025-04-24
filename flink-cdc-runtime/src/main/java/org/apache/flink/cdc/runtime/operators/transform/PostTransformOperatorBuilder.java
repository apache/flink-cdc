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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Builder of {@link PostTransformOperator}. */
public class PostTransformOperatorBuilder {
    private final List<TransformRule> transformRules = new ArrayList<>();
    private String timezone;
    private final List<Tuple3<String, String, Map<String, String>>> udfFunctions =
            new ArrayList<>();

    public PostTransformOperatorBuilder addTransform(
            String tableInclusions,
            @Nullable String projection,
            @Nullable String filter,
            String primaryKey,
            String partitionKey,
            String tableOptions,
            String postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        transformRules.add(
                new TransformRule(
                        tableInclusions,
                        projection,
                        filter,
                        primaryKey,
                        partitionKey,
                        tableOptions,
                        postTransformConverter,
                        supportedMetadataColumns));
        return this;
    }

    public PostTransformOperatorBuilder addTransform(
            String tableInclusions, @Nullable String projection, @Nullable String filter) {
        transformRules.add(
                new TransformRule(
                        tableInclusions,
                        projection,
                        filter,
                        "",
                        "",
                        "",
                        null,
                        new SupportedMetadataColumn[0]));
        return this;
    }

    public PostTransformOperatorBuilder addTimezone(String timezone) {
        if (PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue().equals(timezone)) {
            this.timezone = ZoneId.systemDefault().toString();
        } else {
            this.timezone = timezone;
        }
        return this;
    }

    public PostTransformOperatorBuilder addUdfFunctions(
            List<Tuple3<String, String, Map<String, String>>> udfFunctions) {
        this.udfFunctions.addAll(udfFunctions);
        return this;
    }

    public PostTransformOperator build() {
        return new PostTransformOperator(transformRules, timezone, udfFunctions);
    }
}
