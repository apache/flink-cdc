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

package org.apache.flink.cdc.connectors.fluss.source.split;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import org.apache.fluss.metadata.PhysicalTablePath;
import org.apache.fluss.metadata.TableBucket;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.types.RowType;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Serializer for {@link FlussSplitBase} and its subclasses. Uses a type tag byte to distinguish
 * between split types: log (0) and hybrid snapshot-log (1).
 */
public class FlussSplitSerializer implements SimpleVersionedSerializer<FlussSplitBase> {

    private static final int VERSION = 1;

    private static final byte TYPE_LOG = 0;
    private static final byte TYPE_HYBRID = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(FlussSplitBase split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {

            // Write type tag
            if (split.isHybridSnapshotLogSplit()) {
                out.writeByte(TYPE_HYBRID);
            } else if (split.isLogSplit()) {
                out.writeByte(TYPE_LOG);
            } else {
                throw new IOException(
                        "Unsupported split type: " + split.getClass().getSimpleName());
            }

            // Write common fields
            out.writeUTF(split.getPhysicalTablePath().getDatabaseName());
            out.writeUTF(split.getPhysicalTablePath().getTableName());
            out.writeLong(split.getTableBucket().getTableId());
            out.writeInt(split.getTableBucket().getBucket());

            Long partitionId = split.getTableBucket().getPartitionId();
            out.writeBoolean(partitionId != null);
            if (partitionId != null) {
                out.writeLong(partitionId);
            }
            String partitionName = split.getPhysicalTablePath().getPartitionName();
            out.writeBoolean(partitionName != null);
            if (partitionName != null) {
                out.writeUTF(partitionName);
            }

            // Write type-specific fields
            if (split.isHybridSnapshotLogSplit()) {
                FlussHybridSnapshotLogSplit hybrid = split.asHybridSnapshotLogSplit();
                out.writeLong(hybrid.getSnapshotId());
                out.writeLong(hybrid.getRecordsToSkip());
                out.writeLong(hybrid.getLogStartingOffset());
                out.writeBoolean(hybrid.isSnapshotFinished());
            } else {
                FlussLogSplit log = split.asLogSplit();
                out.writeLong(log.getStartingOffset());
            }

            // Write schema info
            writeSchemaInfo(out, split.getSchemaId(), split.getRowType());

            return baos.toByteArray();
        }
    }

    @Override
    public FlussSplitBase deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais)) {

            byte type = in.readByte();

            // Read common fields
            String databaseName = in.readUTF();
            String tableName = in.readUTF();
            long tableId = in.readLong();
            int bucket = in.readInt();

            boolean hasPartitionId = in.readBoolean();
            Long partitionId = hasPartitionId ? in.readLong() : null;

            boolean hasPartitionName = in.readBoolean();
            String partitionName = hasPartitionName ? in.readUTF() : null;

            TablePath tablePath = new TablePath(databaseName, tableName);
            PhysicalTablePath physicalTablePath =
                    partitionName != null
                            ? PhysicalTablePath.of(tablePath, partitionName)
                            : PhysicalTablePath.of(tablePath);
            TableBucket tableBucket =
                    partitionId != null
                            ? new TableBucket(tableId, partitionId, bucket)
                            : new TableBucket(tableId, bucket);

            // Read type-specific fields and schema info
            @Nullable Integer schemaId = null;
            @Nullable RowType rowType = null;

            switch (type) {
                case TYPE_HYBRID:
                    {
                        long snapshotId = in.readLong();
                        long recordsToSkip = in.readLong();
                        long logStartingOffset = in.readLong();
                        boolean snapshotFinished = in.readBoolean();
                        schemaId = readSchemaId(in);
                        rowType = readRowType(in);
                        return new FlussHybridSnapshotLogSplit(
                                physicalTablePath,
                                tableBucket,
                                snapshotId,
                                recordsToSkip,
                                logStartingOffset,
                                snapshotFinished,
                                schemaId,
                                rowType);
                    }
                case TYPE_LOG:
                    {
                        long startingOffset = in.readLong();
                        schemaId = readSchemaId(in);
                        rowType = readRowType(in);
                        return new FlussLogSplit(
                                physicalTablePath, tableBucket, startingOffset, schemaId, rowType);
                    }
                default:
                    throw new IOException("Unknown split type: " + type);
            }
        }
    }

    // -------------------------------------------------------------------------
    //  Schema info serialization helpers
    // -------------------------------------------------------------------------

    private static void writeSchemaInfo(
            DataOutputViewStreamWrapper out, @Nullable Integer schemaId, @Nullable RowType rowType)
            throws IOException {
        out.writeBoolean(schemaId != null);
        if (schemaId != null) {
            out.writeInt(schemaId);
        }
        out.writeBoolean(rowType != null);
        if (rowType != null) {
            ByteArrayOutputStream rowTypeBaos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(rowTypeBaos);
            oos.writeObject(rowType);
            oos.flush();
            byte[] rowTypeBytes = rowTypeBaos.toByteArray();
            out.writeInt(rowTypeBytes.length);
            out.write(rowTypeBytes);
        }
    }

    private static @Nullable Integer readSchemaId(DataInputViewStreamWrapper in)
            throws IOException {
        boolean hasSchemaId = in.readBoolean();
        return hasSchemaId ? in.readInt() : null;
    }

    private static @Nullable RowType readRowType(DataInputViewStreamWrapper in) throws IOException {
        boolean hasRowType = in.readBoolean();
        if (!hasRowType) {
            return null;
        }
        try {
            int len = in.readInt();
            byte[] rowTypeBytes = new byte[len];
            in.readFully(rowTypeBytes);
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(rowTypeBytes));
            return (RowType) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize RowType", e);
        }
    }
}
