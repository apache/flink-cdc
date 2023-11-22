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



import com.ververica.cdc.common.data.DecimalData;
import com.ververica.cdc.common.data.GenericArrayData;
import com.ververica.cdc.common.data.GenericMapData;
import com.ververica.cdc.common.data.GenericRecordData;
import com.ververica.cdc.common.data.GenericStringData;
import com.ververica.cdc.common.data.TimestampData;
import com.ververica.cdc.common.types.DataTypes;

import com.ververica.cdc.common.schema.Column;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DorisRowConverterTest {

    @Test
    public void testExternalConvert() {
        List<Column> columns = Arrays.asList(
                Column.physicalColumn("f2", DataTypes.BOOLEAN()),
                Column.physicalColumn("f3", DataTypes.FLOAT()),
                Column.physicalColumn("f4", DataTypes.DOUBLE()),
                Column.physicalColumn("f5", DataTypes.ARRAY(DataTypes.STRING())),
                Column.physicalColumn("f6", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
                Column.physicalColumn("f7", DataTypes.TINYINT()),
                Column.physicalColumn("f8", DataTypes.SMALLINT()),
                Column.physicalColumn("f9", DataTypes.INT()),
                Column.physicalColumn("f10", DataTypes.BIGINT()),
                Column.physicalColumn("f11", DataTypes.DECIMAL(10,2)),
                Column.physicalColumn("f12", DataTypes.TIMESTAMP()),
                Column.physicalColumn("f13", DataTypes.TIMESTAMP_LTZ()),
                Column.physicalColumn("f14", DataTypes.DATE()),
                Column.physicalColumn("f15", DataTypes.CHAR(1)),
                Column.physicalColumn("f16", DataTypes.VARCHAR(256)));

        GenericArrayData arrayData = new GenericArrayData(new String[]{"1","2"});
        Map<String, String> map = new HashMap<>();
        map.put("A", "1");
        GenericMapData mapData = new GenericMapData(map);
        LocalDateTime time1 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDateTime time2 = LocalDateTime.of(2021, 1, 1, 8, 0, 0);
        LocalDate date1 = LocalDate.of(2021, 1, 1);
        GenericRecordData recordData = GenericRecordData.of(true, 1.2F, 1.2345D, arrayData, mapData, (byte) 1, (short) 32, 64, 128L,
                DecimalData.fromBigDecimal(BigDecimal.valueOf(10.123), 5, 3),
                TimestampData.fromLocalDateTime(time1), TimestampData.fromLocalDateTime(time2),
                (int) date1.toEpochDay(), GenericStringData.fromString("a"), GenericStringData.fromString("doris"));
        List row = new ArrayList();
        for (int i = 0; i < recordData.getArity(); i++) {
            DorisRowConverter.SerializationConverter converter = DorisRowConverter.createNullableExternalConverter(columns.get(i).getType());
            row.add(converter.serialize(i, recordData));
        }
        Assert.assertEquals("[true, 1.2, 1.2345, [1, 2], {\"A\":\"1\"}, 1, 32, 64, 128, 10.123, 2021-01-01 08:00:00.0, 2021-01-01 08:00:00.0, 2021-01-01, a, doris]", row.toString());
    }
}
