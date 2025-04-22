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

package org.apache.flink.cdc.runtime.operators.schema.regular.event;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaOperator;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;

import java.util.Objects;

/**
 * The response for {@link OriginalSchemaRequest} from {@link SchemaCoordinator} to {@link
 * SchemaOperator}.
 */
public class OriginalSchemaResponse implements CoordinationResponse {
    private static final long serialVersionUID = 1L;

    private final Schema schema;

    public static OriginalSchemaResponse success(Schema schema) {
        return new OriginalSchemaResponse(schema);
    }

    private OriginalSchemaResponse(Schema schema) {
        this.schema = schema;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OriginalSchemaResponse)) {
            return false;
        }
        OriginalSchemaResponse response = (OriginalSchemaResponse) o;
        return Objects.equals(schema, response.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public String toString() {
        return "OriginalSchemaResponse{schema=" + schema + '}';
    }
}
