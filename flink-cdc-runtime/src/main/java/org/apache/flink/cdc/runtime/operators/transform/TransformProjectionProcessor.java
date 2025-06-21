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

import org.apache.flink.cdc.common.source.SupportedMetadataColumn;
import org.apache.flink.cdc.common.utils.Preconditions;
import org.apache.flink.cdc.runtime.parser.TransformParser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * The processor of transform projection applies to process a row of filtering tables.
 *
 * <p>A transform projection processor contains:
 *
 * <ul>
 *   <li>CreateTableEvent: add the user-defined computed columns into Schema.
 *   <li>SchemaChangeEvent: update the columns of TransformProjection.
 *   <li>DataChangeEvent: Fill data field to row in PreTransformOperator. Process the data column
 *       and the user-defined expression computed columns.
 * </ul>
 */
public class TransformProjectionProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(TransformProjectionProcessor.class);

    private final PostTransformChangeInfo changeInfo;
    private final String projectionExpression;
    private final String timezone;
    private final List<UserDefinedFunctionDescriptor> udfDescriptors;
    private final List<Object> udfFunctionInstances;
    private final List<ProjectionColumnProcessor> columnProcessors;
    private final SupportedMetadataColumn[] supportedMetadataColumns;
    private final Map<String, SupportedMetadataColumn> supportedMetadataColumnsMap;

    public TransformProjectionProcessor(
            PostTransformChangeInfo changeInfo,
            String projectionExpression,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances,
            SupportedMetadataColumn[] supportedMetadataColumns) {
        this.changeInfo = changeInfo;
        this.projectionExpression = projectionExpression;
        this.timezone = timezone;
        this.udfDescriptors = udfDescriptors;
        this.udfFunctionInstances = udfFunctionInstances;
        this.supportedMetadataColumns = supportedMetadataColumns;

        // Construct a mapping table ad-hoc to accelerate looking-up
        Map<String, SupportedMetadataColumn> supportedMetadataColumnsMap = new HashMap<>();
        for (SupportedMetadataColumn supportedMetadataColumn : supportedMetadataColumns) {
            supportedMetadataColumnsMap.put(
                    supportedMetadataColumn.getName(), supportedMetadataColumn);
        }
        this.supportedMetadataColumnsMap = supportedMetadataColumnsMap;
        this.columnProcessors = createProjectionColumnProcessors();
    }

    public Object[] project(Object[] rowData, TransformContext context) {
        return columnProcessors.stream()
                .map(processor -> processor.evaluate(rowData, context))
                .toArray();
    }

    private List<ProjectionColumnProcessor> createProjectionColumnProcessors() {
        Preconditions.checkNotNull(
                changeInfo,
                "Projection column processors could only be created if changeInfo is available.");

        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        projectionExpression,
                        changeInfo.getPreTransformedSchema().getColumns(),
                        udfDescriptors,
                        supportedMetadataColumns);

        List<ProjectionColumnProcessor> columnProcessors =
                projectionColumns.stream()
                        .map(
                                column ->
                                        ProjectionColumnProcessor.of(
                                                changeInfo,
                                                column,
                                                timezone,
                                                udfDescriptors,
                                                udfFunctionInstances,
                                                supportedMetadataColumnsMap))
                        .collect(Collectors.toList());

        LOG.info("Successfully created projection column processors cache.");
        LOG.info("Cached results: {}", columnProcessors);
        return columnProcessors;
    }
}
