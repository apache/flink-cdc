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

package org.apache.flink.cdc.connectors.elasticsearch.sink;

import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.sink.MetadataApplier;

/**
 * A metadata applier for Elasticsearch that handles schema change events. This class is responsible
 * for applying metadata changes to the Elasticsearch index based on schema change events from the
 * CDC source.
 */
public class ElasticsearchMetadataApplier implements MetadataApplier {

    /**
     * Applies the given schema change event to the Elasticsearch index.
     *
     * @param event The schema change event to apply.
     */
    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        // TODO: Implement the logic to apply schema changes to Elasticsearch
        // This might include:
        // - Creating new indices
        // - Updating existing index mappings
        // - Handling column additions or deletions
        throw new UnsupportedOperationException(
                "Schema change application is not yet implemented for Elasticsearch.");
    }
}
