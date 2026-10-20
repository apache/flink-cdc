/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.paimon.sink.v2.blob;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.Blob;
import org.apache.paimon.data.BlobData;
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.data.BlobRef;
import org.apache.paimon.table.FileStoreTable;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.paimon.utils.StringUtils.toLowerCaseIfNeed;

/**
 * Context for handling BLOB fields during CDC write operations.
 *
 * <p>CDC connector only handles WRITE operations. We do NOT read external blob data. The actual
 * blob data reading happens on Paimon read side.
 *
 * <p>Two blob storage modes for CDC write:
 *
 * <ul>
 *   <li>Mode 1 (raw data): VARBINARY/BINARY fields → BlobData → written to .blob files by Paimon.
 *   <li>Mode 2 (descriptor): VARCHAR/STRING fields → BlobRef → only descriptor (uri, offset,
 *       length) stored inline. External data is NOT read or copied during write.
 * </ul>
 */
public class BlobWriteContext implements Serializable {

    private static final long serialVersionUID = 1L;

    private final boolean isCaseSensitive;

    /** Fields that should be converted to BLOB type. */
    private final Set<String> blobFields;

    /** Fields configured with blob-descriptor-field (Mode 2). */
    private final Set<String> blobDescriptorFields;

    private BlobWriteContext(
            boolean isCaseSensitive, Set<String> blobFields, Set<String> blobDescriptorFields) {
        this.isCaseSensitive = isCaseSensitive;
        // If case-insensitive, normalize field names to lowercase for consistent matching
        this.blobFields = downcaseFieldNames(blobFields, isCaseSensitive);
        this.blobDescriptorFields = downcaseFieldNames(blobDescriptorFields, isCaseSensitive);
    }

    /** Downcase field names if case-insensitive. */
    private static Set<String> downcaseFieldNames(Set<String> fieldNames, boolean caseSensitive) {
        if (caseSensitive) {
            return fieldNames;
        }
        Set<String> normalized = new HashSet<>();
        for (String fieldName : fieldNames) {
            normalized.add(fieldName.toLowerCase());
        }
        return normalized;
    }

    /** Create BlobWriteContext from Paimon table's CoreOptions. */
    public static BlobWriteContext fromTable(boolean isCaseSensitive, FileStoreTable table) {
        List<String> blobFieldList = CoreOptions.blobField(table.options());
        if (blobFieldList.isEmpty()) {
            return empty();
        }
        CoreOptions coreOptions = CoreOptions.fromMap(table.options());
        Set<String> blobDescriptorFields = coreOptions.blobDescriptorField();
        return new BlobWriteContext(
                isCaseSensitive, new HashSet<>(blobFieldList), blobDescriptorFields);
    }

    /** Create an empty BlobWriteContext with no blob fields. */
    public static BlobWriteContext empty() {
        return new BlobWriteContext(true, Collections.emptySet(), Collections.emptySet());
    }

    /** Check if a field should be converted to BLOB type. */
    public boolean isBlobField(String fieldName) {
        return blobFields.contains(toLowerCaseIfNeed(fieldName, isCaseSensitive));
    }

    /** Get the set of blob field names. */
    public Set<String> getBlobFields() {
        return blobFields;
    }

    /** Check if a field is configured with blob-descriptor-field. */
    public boolean isBlobDescriptorField(String fieldName) {
        return blobDescriptorFields.contains(toLowerCaseIfNeed(fieldName, isCaseSensitive));
    }

    /** Get the set of blob descriptor field names. */
    public Set<String> getBlobDescriptorFields() {
        return blobDescriptorFields;
    }

    /** Create BlobData from raw bytes (Mode 1). */
    @Nullable
    public Blob createBlob(@Nullable byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        return new BlobData(bytes);
    }

    /** Create BlobRef from a path string (Mode 2). */
    @Nullable
    public Blob createBlobRef(@Nullable String path) {
        return createBlobRef(path, -1);
    }

    /** Create BlobRef from a path string with specified length (Mode 2). */
    @Nullable
    public Blob createBlobRef(@Nullable String path, long length) {
        if (path == null) {
            return null;
        }
        BlobDescriptor descriptor = new BlobDescriptor(path, 0, length);
        // UriReader is null - CDC connector only writes descriptor info, not reading data
        return new BlobRef(null, descriptor);
    }
}
