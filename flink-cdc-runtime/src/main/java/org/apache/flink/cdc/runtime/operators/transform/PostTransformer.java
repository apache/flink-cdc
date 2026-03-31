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

import org.apache.flink.cdc.common.pipeline.SchemaColumnCaseFormat;
import org.apache.flink.cdc.common.schema.Selectors;
import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.runtime.operators.transform.converter.PostTransformConverter;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Post-Transformation rule used by {@link PostTransformOperator}. */
public class PostTransformer {
    private final Selectors selectors;

    private final @Nullable TransformProjection projection;
    private final @Nullable TransformFilter filter;
    private final @Nullable PostTransformConverter postTransformConverter;
    private final SupportedMetadataColumn[] supportedMetadataColumns;

    /**
     * When non-empty, overrides automatic primary-key name remapping after projection (YAML {@code
     * primary-keys}). Names refer to <b>post-projection</b> columns.
     */
    private final List<String> explicitPrimaryKeys;

    /**
     * When non-empty, overrides automatic partition-key name remapping (YAML {@code
     * partition-keys}). Names refer to <b>post-projection</b> columns.
     */
    private final List<String> explicitPartitionKeys;

    private final SchemaColumnCaseFormat schemaColumnCaseFormat;

    public PostTransformer(
            Selectors selectors,
            @Nullable TransformProjection projection,
            @Nullable TransformFilter filter,
            @Nullable PostTransformConverter postTransformConverter,
            SupportedMetadataColumn[] supportedMetadataColumns,
            List<String> explicitPrimaryKeys,
            List<String> explicitPartitionKeys,
            SchemaColumnCaseFormat schemaColumnCaseFormat) {
        this.selectors = selectors;
        this.projection = projection;
        this.filter = filter;
        this.postTransformConverter = postTransformConverter;
        this.supportedMetadataColumns = supportedMetadataColumns;
        this.explicitPrimaryKeys =
                explicitPrimaryKeys == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(explicitPrimaryKeys);
        this.explicitPartitionKeys =
                explicitPartitionKeys == null
                        ? Collections.emptyList()
                        : Collections.unmodifiableList(explicitPartitionKeys);
        this.schemaColumnCaseFormat = schemaColumnCaseFormat;
    }

    public Selectors getSelectors() {
        return selectors;
    }

    public Optional<TransformProjection> getProjection() {
        return Optional.ofNullable(projection);
    }

    public Optional<TransformFilter> getFilter() {
        return Optional.ofNullable(filter);
    }

    public Optional<PostTransformConverter> getPostTransformConverter() {
        return Optional.ofNullable(postTransformConverter);
    }

    public SupportedMetadataColumn[] getSupportedMetadataColumns() {
        return supportedMetadataColumns;
    }

    public List<String> getExplicitPrimaryKeys() {
        return explicitPrimaryKeys;
    }

    public List<String> getExplicitPartitionKeys() {
        return explicitPartitionKeys;
    }

    public SchemaColumnCaseFormat getSchemaColumnCaseFormat() {
        return schemaColumnCaseFormat;
    }
}
