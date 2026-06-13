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

package org.apache.flink.cdc.connectors.lancedb.client;

import org.apache.flink.cdc.connectors.lancedb.sink.LanceDbDataSinkConfig;
import org.apache.flink.cdc.connectors.lancedb.utils.LanceDbRetryUtils;

import com.lancedb.lance.Dataset;
import com.lancedb.lance.Fragment;
import com.lancedb.lance.FragmentMetadata;
import com.lancedb.lance.Transaction;
import com.lancedb.lance.WriteParams;
import com.lancedb.lance.operation.Append;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.holders.NullableBigIntHolder;
import org.apache.arrow.vector.holders.NullableBitHolder;
import org.apache.arrow.vector.holders.NullableFloat4Holder;
import org.apache.arrow.vector.holders.NullableFloat8Holder;
import org.apache.arrow.vector.holders.NullableIntHolder;
import org.apache.arrow.vector.holders.NullableSmallIntHolder;
import org.apache.arrow.vector.holders.NullableTinyIntHolder;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Default Lance Java client implementation. */
public class DefaultLanceDbClient implements LanceDbClient {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultLanceDbClient.class);

    private final LanceDbDataSinkConfig config;
    private final BufferAllocator allocator;
    private final Map<String, Dataset> datasets;

    public DefaultLanceDbClient(LanceDbDataSinkConfig config) {
        this.config = config;
        this.allocator = new RootAllocator(Long.MAX_VALUE);
        this.datasets = new HashMap<>();
    }

    @Override
    public boolean datasetExists(String datasetPath) {
        try {
            Dataset dataset = openDataset(datasetPath);
            return dataset != null && !dataset.closed();
        } catch (LinkageError e) {
            throw wrapNativeFailure(e);
        } catch (RuntimeException e) {
            if (LanceDbRetryUtils.isDatasetNotFound(e)) {
                return false;
            }
            throw e;
        }
    }

    @Override
    public Schema getSchema(String datasetPath) {
        return openDataset(datasetPath).getSchema();
    }

    @Override
    public void createDataset(String datasetPath, Schema schema) {
        ensureLocalParentExists(datasetPath);
        Dataset dataset =
                withNativeFailureHandling(
                        () ->
                                Dataset.create(
                                        allocator,
                                        datasetPath,
                                        schema,
                                        buildWriteParams(config.getWriteMode())));
        replaceDataset(datasetPath, dataset);
    }

    @Override
    public void addColumns(String datasetPath, List<Field> fields) {
        try {
            openDataset(datasetPath).addColumns(fields);
            reopen(datasetPath);
        } catch (LinkageError e) {
            throw wrapNativeFailure(e);
        }
    }

    @Override
    public int appendRows(String datasetPath, Schema schema, List<List<Object>> rows) {
        if (rows.isEmpty()) {
            return 0;
        }
        try {
            Dataset dataset = openDataset(datasetPath);
            try (VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
                root.allocateNew();
                for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
                    List<Object> row = rows.get(rowIndex);
                    if (row.size() != schema.getFields().size()) {
                        throw new IllegalArgumentException(
                                "Lance row has "
                                        + row.size()
                                        + " values but schema has "
                                        + schema.getFields().size()
                                        + " fields.");
                    }
                    for (int columnIndex = 0;
                            columnIndex < schema.getFields().size();
                            columnIndex++) {
                        Field field = schema.getFields().get(columnIndex);
                        setVectorValue(
                                root.getVector(field.getName()),
                                field,
                                row.get(columnIndex),
                                rowIndex);
                    }
                }
                root.setRowCount(rows.size());
                List<FragmentMetadata> fragments =
                        Fragment.create(datasetPath, allocator, root, buildWriteParams("APPEND"));
                if (!fragments.isEmpty()) {
                    Transaction transaction =
                            dataset.newTransactionBuilder()
                                    .operation(Append.builder().fragments(fragments).build())
                                    .build();
                    replaceDataset(datasetPath, transaction.commit());
                }
            }
        } catch (LinkageError e) {
            throw wrapNativeFailure(e);
        }
        return rows.size();
    }

    @Override
    public void reopen(String datasetPath) {
        replaceDataset(
                datasetPath, withNativeFailureHandling(() -> Dataset.open(datasetPath, allocator)));
    }

    @Override
    public void close() {
        RuntimeException failure = null;
        for (Dataset dataset : datasets.values()) {
            try {
                dataset.close();
            } catch (RuntimeException e) {
                failure = e;
            }
        }
        datasets.clear();
        try {
            allocator.close();
        } catch (RuntimeException e) {
            failure = e;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private Dataset openDataset(String datasetPath) {
        try {
            Dataset dataset = datasets.get(datasetPath);
            if (dataset != null && !dataset.closed()) {
                return dataset;
            }
            dataset = Dataset.open(datasetPath, allocator);
            datasets.put(datasetPath, dataset);
            return dataset;
        } catch (LinkageError e) {
            throw wrapNativeFailure(e);
        }
    }

    private void replaceDataset(String datasetPath, Dataset dataset) {
        Dataset current = datasets.put(datasetPath, dataset);
        if (current != null && current != dataset) {
            try {
                current.close();
            } catch (RuntimeException e) {
                LOG.warn("Failed to close stale Lance dataset handle for {}.", datasetPath, e);
            }
        }
    }

    private WriteParams buildWriteParams(String mode) {
        return new WriteParams.Builder()
                .withMaxRowsPerFile(config.getMaxRowsPerFile())
                .withMaxRowsPerGroup(config.getMaxRowsPerGroup())
                .withMaxBytesPerFile(config.getMaxBytesPerFile())
                .withEnableStableRowIds(config.isEnableStableRowIds())
                .withMode(WriteParams.WriteMode.valueOf(mode))
                .withStorageOptions(config.getStorageOptions())
                .build();
    }

    private void ensureLocalParentExists(String datasetPath) {
        if (datasetPath.contains("://")) {
            return;
        }
        Path parent = Paths.get(datasetPath).toAbsolutePath().getParent();
        if (parent == null) {
            return;
        }
        try {
            Files.createDirectories(parent);
        } catch (java.io.IOException e) {
            throw new RuntimeException(
                    "Failed to create parent directory for Lance dataset " + datasetPath + ".", e);
        }
    }

    private Dataset withNativeFailureHandling(DatasetSupplier supplier) {
        try {
            return supplier.get();
        } catch (LinkageError e) {
            throw wrapNativeFailure(e);
        }
    }

    private RuntimeException wrapNativeFailure(Throwable failure) {
        return new IllegalStateException(
                "Failed to load or execute Lance Java native code. Ensure the JVM architecture is supported by lance-core and matches the host. For example, macOS arm64 must use an arm64 JDK; Linux x86_64 and Linux aarch64 are supported by the bundled lance-core native libraries. Also verify shaded Arrow/Lance dependencies are present in the connector jar.",
                failure);
    }

    private interface DatasetSupplier {
        Dataset get();
    }

    private void setVectorValue(FieldVector vector, Field field, Object value, int rowIndex) {
        if (value == null) {
            vector.setNull(rowIndex);
            return;
        }
        ArrowType arrowType = field.getType();
        if (arrowType instanceof ArrowType.Int) {
            setIntValue(vector, (ArrowType.Int) arrowType, value, rowIndex);
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            setFloatingPointValue(vector, (ArrowType.FloatingPoint) arrowType, value, rowIndex);
        } else if (arrowType instanceof ArrowType.Bool) {
            NullableBitHolder holder = new NullableBitHolder();
            holder.isSet = 1;
            holder.value = Boolean.TRUE.equals(value) ? 1 : 0;
            ((BitVector) vector).setSafe(rowIndex, holder);
        } else if (arrowType instanceof ArrowType.Utf8) {
            ((VarCharVector) vector)
                    .setSafe(rowIndex, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (arrowType instanceof ArrowType.Binary) {
            ((VarBinaryVector) vector).setSafe(rowIndex, (byte[]) value);
        } else if (arrowType instanceof ArrowType.Decimal) {
            ((DecimalVector) vector).setSafe(rowIndex, (BigDecimal) value);
        } else if (arrowType instanceof ArrowType.Date) {
            ((DateDayVector) vector).setSafe(rowIndex, ((Number) value).intValue());
        } else if (arrowType instanceof ArrowType.Time) {
            ((TimeMilliVector) vector).setSafe(rowIndex, ((Number) value).intValue());
        } else if (arrowType instanceof ArrowType.Timestamp) {
            ((TimeStampMicroVector) vector).setSafe(rowIndex, ((Number) value).longValue());
        } else if (arrowType instanceof ArrowType.List) {
            setListValue((ListVector) vector, field, value, rowIndex);
        } else {
            throw new UnsupportedOperationException("Unsupported Arrow type: " + arrowType);
        }
    }

    private void setIntValue(
            FieldVector vector, ArrowType.Int arrowType, Object value, int rowIndex) {
        switch (arrowType.getBitWidth()) {
            case 8:
                NullableTinyIntHolder tinyIntHolder = new NullableTinyIntHolder();
                tinyIntHolder.isSet = 1;
                tinyIntHolder.value = ((Number) value).byteValue();
                ((TinyIntVector) vector).setSafe(rowIndex, tinyIntHolder);
                break;
            case 16:
                NullableSmallIntHolder smallIntHolder = new NullableSmallIntHolder();
                smallIntHolder.isSet = 1;
                smallIntHolder.value = ((Number) value).shortValue();
                ((SmallIntVector) vector).setSafe(rowIndex, smallIntHolder);
                break;
            case 32:
                NullableIntHolder intHolder = new NullableIntHolder();
                intHolder.isSet = 1;
                intHolder.value = ((Number) value).intValue();
                ((IntVector) vector).setSafe(rowIndex, intHolder);
                break;
            case 64:
                NullableBigIntHolder bigIntHolder = new NullableBigIntHolder();
                bigIntHolder.isSet = 1;
                bigIntHolder.value = ((Number) value).longValue();
                ((BigIntVector) vector).setSafe(rowIndex, bigIntHolder);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Arrow integer width: " + arrowType.getBitWidth());
        }
    }

    private void setFloatingPointValue(
            FieldVector vector, ArrowType.FloatingPoint arrowType, Object value, int rowIndex) {
        switch (arrowType.getPrecision()) {
            case SINGLE:
                NullableFloat4Holder floatHolder = new NullableFloat4Holder();
                floatHolder.isSet = 1;
                floatHolder.value = ((Number) value).floatValue();
                ((Float4Vector) vector).setSafe(rowIndex, floatHolder);
                break;
            case DOUBLE:
                NullableFloat8Holder doubleHolder = new NullableFloat8Holder();
                doubleHolder.isSet = 1;
                doubleHolder.value = ((Number) value).doubleValue();
                ((Float8Vector) vector).setSafe(rowIndex, doubleHolder);
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported Arrow floating point precision: " + arrowType.getPrecision());
        }
    }

    private void setListValue(FieldVector vector, Field field, Object value, int rowIndex) {
        if (!(value instanceof List)) {
            throw new IllegalArgumentException(
                    "Lance list value must be java.util.List but was " + value.getClass());
        }
        if (field.getChildren().isEmpty()) {
            throw new IllegalArgumentException(
                    "Lance list field must have an element child field.");
        }
        Field elementField = field.getChildren().get(0);
        UnionListWriter writer = ((ListVector) vector).getWriter();
        writer.setPosition(rowIndex);
        writer.startList();
        for (Object element : (List<?>) value) {
            writeListElement(writer, elementField.getType(), element);
        }
        writer.endList();
    }

    private void writeListElement(UnionListWriter writer, ArrowType arrowType, Object value) {
        if (value == null) {
            writer.writeNull();
            return;
        }
        if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 64) {
                writer.bigInt().writeBigInt(((Number) value).longValue());
            } else {
                writer.integer().writeInt(((Number) value).intValue());
            }
        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            ArrowType.FloatingPoint floatingPoint = (ArrowType.FloatingPoint) arrowType;
            switch (floatingPoint.getPrecision()) {
                case SINGLE:
                    writer.float4().writeFloat4(((Number) value).floatValue());
                    break;
                case DOUBLE:
                    writer.float8().writeFloat8(((Number) value).doubleValue());
                    break;
                default:
                    throw new UnsupportedOperationException(
                            "Unsupported list floating point precision: "
                                    + floatingPoint.getPrecision());
            }
        } else if (arrowType instanceof ArrowType.Bool) {
            writer.bit().writeBit(Boolean.TRUE.equals(value) ? 1 : 0);
        } else if (arrowType instanceof ArrowType.Utf8) {
            byte[] bytes = value.toString().getBytes(StandardCharsets.UTF_8);
            try (ArrowBuf buffer = allocator.buffer(bytes.length)) {
                buffer.writeBytes(bytes);
                writer.varChar().writeVarChar(0, bytes.length, buffer);
            }
        } else {
            throw new UnsupportedOperationException(
                    "Unsupported Lance list element Arrow type: " + arrowType);
        }
    }
}
