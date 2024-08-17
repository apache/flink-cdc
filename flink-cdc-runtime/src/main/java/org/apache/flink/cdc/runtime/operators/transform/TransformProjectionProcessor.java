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

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.data.binary.BinaryRecordData;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.cdc.runtime.parser.TransformParser;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
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
    private final PostTransformChangeInfo postTransformChangeInfo;
    private final TransformProjection transformProjection;
    private final String timezone;
    private final List<ProjectionColumnProcessor> cachedProjectionColumnProcessors;
    private final List<UserDefinedFunctionDescriptor> udfDescriptors;
    private final transient List<Object> udfFunctionInstances;

    public TransformProjectionProcessor(
            PostTransformChangeInfo postTransformChangeInfo,
            TransformProjection transformProjection,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            final List<Object> udfFunctionInstances) {
        this.postTransformChangeInfo = postTransformChangeInfo;
        this.transformProjection = transformProjection;
        this.timezone = timezone;
        this.udfDescriptors = udfDescriptors;
        this.udfFunctionInstances = udfFunctionInstances;

        // Create cached projection column processors after setting all other fields.
        this.cachedProjectionColumnProcessors =
                cacheProjectionColumnProcessors(postTransformChangeInfo, transformProjection);
    }

    public boolean hasTableInfo() {
        return this.postTransformChangeInfo != null;
    }

    public static TransformProjectionProcessor of(
            PostTransformChangeInfo tableInfo,
            TransformProjection transformProjection,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances) {
        return new TransformProjectionProcessor(
                tableInfo, transformProjection, timezone, udfDescriptors, udfFunctionInstances);
    }

    public static TransformProjectionProcessor of(
            TransformProjection transformProjection,
            String timezone,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances) {
        return new TransformProjectionProcessor(
                null, transformProjection, timezone, udfDescriptors, udfFunctionInstances);
    }

    public static TransformProjectionProcessor of(
            TransformProjection transformProjection,
            List<UserDefinedFunctionDescriptor> udfDescriptors,
            List<Object> udfFunctionInstances) {
        return new TransformProjectionProcessor(
                null, transformProjection, null, udfDescriptors, udfFunctionInstances);
    }

    public Schema processSchemaChangeEvent(Schema schema) {
        List<ProjectionColumn> projectionColumns =
                TransformParser.generateProjectionColumns(
                        transformProjection.getProjection(), schema.getColumns(), udfDescriptors);
        transformProjection.setProjectionColumns(projectionColumns);
        return schema.copy(
                projectionColumns.stream()
                        .map(ProjectionColumn::getColumn)
                        .collect(Collectors.toList()));
    }

    public BinaryRecordData processData(BinaryRecordData payload, long epochTime, String opType) {
        List<Object> valueList = new ArrayList<>();
        List<Column> columns = postTransformChangeInfo.getPostTransformedSchema().getColumns();

        for (int i = 0; i < columns.size(); i++) {
            ProjectionColumnProcessor projectionColumnProcessor =
                    cachedProjectionColumnProcessors.get(i);
            if (projectionColumnProcessor != null) {
                ProjectionColumn projectionColumn = projectionColumnProcessor.getProjectionColumn();
                valueList.add(
                        DataTypeConverter.convert(
                                projectionColumnProcessor.evaluate(payload, epochTime, opType),
                                projectionColumn.getDataType()));
            } else {
                Column column = columns.get(i);
                valueList.add(
                        getValueFromBinaryRecordData(
                                column.getName(),
                                column.getType(),
                                payload,
                                postTransformChangeInfo.getPreTransformedSchema().getColumns(),
                                postTransformChangeInfo.getPreTransformedFieldGetters()));
            }
        }

        return postTransformChangeInfo
                .getRecordDataGenerator()
                .generate(valueList.toArray(new Object[0]));
    }

    private Object getValueFromBinaryRecordData(
            String columnName,
            DataType expectedType,
            BinaryRecordData binaryRecordData,
            List<Column> columns,
            RecordData.FieldGetter[] fieldGetters) {
        for (int i = 0; i < columns.size(); i++) {
            if (columnName.equals(columns.get(i).getName())) {
                return DataTypeConverter.convert(
                        fieldGetters[i].getFieldOrNull(binaryRecordData), expectedType);
            }
        }
        return null;
    }

    private List<ProjectionColumnProcessor> cacheProjectionColumnProcessors(
            PostTransformChangeInfo tableInfo, TransformProjection transformProjection) {
        List<ProjectionColumnProcessor> cachedProjectionColumnProcessors = new ArrayList<>();
        if (!hasTableInfo()) {
            return cachedProjectionColumnProcessors;
        }

        for (Column column : tableInfo.getPostTransformedSchema().getColumns()) {
            ProjectionColumn matchedProjectionColumn = null;
            for (ProjectionColumn projectionColumn : transformProjection.getProjectionColumns()) {
                if (column.getName().equals(projectionColumn.getColumnName())
                        && projectionColumn.isValidTransformedProjectionColumn()) {
                    matchedProjectionColumn = projectionColumn;
                    break;
                }
            }

            cachedProjectionColumnProcessors.add(
                    Optional.ofNullable(matchedProjectionColumn)
                            .map(
                                    col ->
                                            ProjectionColumnProcessor.of(
                                                    tableInfo,
                                                    col,
                                                    timezone,
                                                    udfDescriptors,
                                                    udfFunctionInstances))
                            .orElse(null));
        }

        return cachedProjectionColumnProcessors;
    }
}
