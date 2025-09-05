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

import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.runtime.typeutils.DataTypeConverter;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;

/** TransformTable to generate the metadata of calcite. */
public class TransformTable extends AbstractTable {

    private String name;

    private List<Column> columns;

    public TransformTable(String name, List<Column> columns) {
        this.name = name;
        this.columns = columns;
    }

    public String getName() {
        return name;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
        return DataTypeConverter.convertCalciteRelDataType(relDataTypeFactory, columns);
    }
}
