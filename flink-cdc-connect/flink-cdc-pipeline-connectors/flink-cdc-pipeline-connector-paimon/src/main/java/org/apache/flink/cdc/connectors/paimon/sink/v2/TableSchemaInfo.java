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

package org.apache.flink.cdc.connectors.paimon.sink.v2;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.connectors.paimon.sink.v2.blob.BlobWriteContext;

import javax.annotation.Nullable;

import java.time.ZoneId;
import java.util.List;

/** Keep a list of {@link RecordData.FieldGetter} for a specific {@link Schema}. */
public class TableSchemaInfo {

    private final Schema schema;

    private List<RecordData.FieldGetter> fieldGetters;

    private final boolean hasPrimaryKey;

    @Nullable private BlobWriteContext blobWriteContext;

    public TableSchemaInfo(Schema schema, ZoneId zoneId) {
        this(schema, zoneId, null);
    }

    public TableSchemaInfo(
            Schema schema, ZoneId zoneId, @Nullable BlobWriteContext blobWriteContext) {
        this.schema = schema;
        this.blobWriteContext = blobWriteContext;
        this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId, blobWriteContext);
        this.hasPrimaryKey = !schema.primaryKeys().isEmpty();
    }

    /**
     * Update the BlobWriteContext and recreate field getters.
     *
     * <p>This is called when the table is accessed in PaimonWriter and we have the actual table
     * configuration.
     */
    public void updateBlobWriteContext(BlobWriteContext blobWriteContext, ZoneId zoneId) {
        if (this.blobWriteContext == null && blobWriteContext == null) {
            return;
        }
        if (this.blobWriteContext != null && blobWriteContext != null) {
            // Compare blob fields and descriptor fields
            boolean sameBlobFields =
                    this.blobWriteContext.getBlobFields().equals(blobWriteContext.getBlobFields());
            boolean sameDescriptorFields =
                    this.blobWriteContext
                            .getBlobDescriptorFields()
                            .equals(blobWriteContext.getBlobDescriptorFields());
            if (sameBlobFields && sameDescriptorFields) {
                return;
            }
        }
        this.blobWriteContext = blobWriteContext;
        this.fieldGetters = PaimonWriterHelper.createFieldGetters(schema, zoneId, blobWriteContext);
    }

    public Schema getSchema() {
        return schema;
    }

    public List<RecordData.FieldGetter> getFieldGetters() {
        return fieldGetters;
    }

    public boolean hasPrimaryKey() {
        return hasPrimaryKey;
    }

    @Nullable
    public BlobWriteContext getBlobWriteContext() {
        return blobWriteContext;
    }
}
