/*
 * Copyright 2023 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.doris.sink;

import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.data.binary.BinaryRecordData;
import com.ververica.cdc.common.data.binary.BinaryStringData;
import com.ververica.cdc.common.schema.Column;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypes;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.runtime.typeutils.BinaryRecordDataGenerator;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/** A test for {@link DorisRowConverter} . */
public class DorisRowConverterTest {

    @Test
    public void testExternalConvert() {
        List<Column> columns =
                Arrays.asList(
                        Column.physicalColumn("f2", DataTypes.BOOLEAN()),
                        Column.physicalColumn("f3", DataTypes.FLOAT()),
                        Column.physicalColumn("f4", DataTypes.DOUBLE()),
                        Column.physicalColumn("f7", DataTypes.TINYINT()),
                        Column.physicalColumn("f8", DataTypes.SMALLINT()),
                        Column.physicalColumn("f9", DataTypes.INT()),
                        Column.physicalColumn("f10", DataTypes.BIGINT()),
                        Column.physicalColumn("f12", DataTypes.TIMESTAMP()),
                        Column.physicalColumn("f14", DataTypes.DATE()),
                        Column.physicalColumn("f15", DataTypes.CHAR(1)),
                        Column.physicalColumn("f16", DataTypes.VARCHAR(256)));

        List<DataType> dataTypes =
                columns.stream().map(v -> v.getType()).collect(Collectors.toList());
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDate date1 = LocalDate.of(2021, 1, 1);

        BinaryRecordDataGenerator generator =
                new BinaryRecordDataGenerator(RowType.of(dataTypes.toArray(new DataType[] {})));
        BinaryRecordData recordData =
                generator.generate(
                        new Object[] {
                            true,
                            1.2F,
                            1.2345D,
                            (byte) 1,
                            (short) 32,
                            64,
                            128L,
                            TimestampData.fromLocalDateTime(time1),
                            (int) date1.toEpochDay(),
                            BinaryStringData.fromString("a"),
                            BinaryStringData.fromString("doris")
                        });
        List row = new ArrayList();
        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter =
                    DorisRowConverter.createNullableExternalConverter(
                            columns.get(i).getType(), ZoneId.systemDefault());
            row.add(converter.serialize(i, recordData));
        }
        Assert.assertEquals(
                "[true, 1.2, 1.2345, 1, 32, 64, 128, 2021-01-01 08:00:00, 2021-01-01, a, doris]",
                row.toString());
    }
}
