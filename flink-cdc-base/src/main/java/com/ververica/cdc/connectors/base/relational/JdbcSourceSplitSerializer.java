/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.base.relational;

import com.ververica.cdc.connectors.base.source.meta.offset.OffsetFactory;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitBase;
import com.ververica.cdc.connectors.base.source.meta.split.SourceSplitSerializer;
import com.ververica.cdc.debezium.history.FlinkJsonTableChangeSerializer;
import io.debezium.document.Document;
import io.debezium.document.DocumentReader;
import io.debezium.document.DocumentWriter;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.io.IOException;

/** A JDBC serializer for the {@link SourceSplitBase}. */
public class JdbcSourceSplitSerializer extends SourceSplitSerializer<TableId, TableChange> {

    private final OffsetFactory offsetFactory;

    public JdbcSourceSplitSerializer(OffsetFactory offsetFactory) {
        this.offsetFactory = offsetFactory;
    }

    @Override
    public OffsetFactory getOffsetFactory() {
        return offsetFactory;
    }

    @Override
    public TableId parseTableId(String identity) {
        return TableId.parse(identity);
    }

    @Override
    protected TableChange readTableSchema(String serialized) throws IOException {
        DocumentReader documentReader = DocumentReader.defaultReader();
        Document document = documentReader.read(serialized);
        return FlinkJsonTableChangeSerializer.fromDocument(document, true);
    }

    @Override
    protected String writeTableSchema(TableChange tableSchema) throws IOException {
        FlinkJsonTableChangeSerializer jsonSerializer = new FlinkJsonTableChangeSerializer();
        DocumentWriter documentWriter = DocumentWriter.defaultWriter();
        return documentWriter.write(jsonSerializer.toDocument(tableSchema));
    }
}
