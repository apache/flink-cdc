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

package org.apache.flink.cdc.runtime.parser.metadata;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/** TransformSchema to generate the metadata of calcite. */
public class TransformSchema extends AbstractSchema {

    private String name;
    private List<TransformTable> tables;

    public TransformSchema(String name, List<TransformTable> tables) {
        this.name = name;
        this.tables = tables;
    }

    @Override
    public Map<String, Table> getTableMap() {
        return tables.stream().collect(Collectors.toMap(TransformTable::getName, t -> t));
    }
}
