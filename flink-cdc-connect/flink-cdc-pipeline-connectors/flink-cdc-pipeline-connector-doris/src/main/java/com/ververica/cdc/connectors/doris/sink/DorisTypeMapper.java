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

import com.ververica.cdc.common.types.ArrayType;
import com.ververica.cdc.common.types.BigIntType;
import com.ververica.cdc.common.types.BooleanType;
import com.ververica.cdc.common.types.CharType;
import com.ververica.cdc.common.types.DataType;
import com.ververica.cdc.common.types.DataTypeChecks;
import com.ververica.cdc.common.types.DataTypeDefaultVisitor;
import com.ververica.cdc.common.types.DateType;
import com.ververica.cdc.common.types.DecimalType;
import com.ververica.cdc.common.types.DoubleType;
import com.ververica.cdc.common.types.FloatType;
import com.ververica.cdc.common.types.IntType;
import com.ververica.cdc.common.types.LocalZonedTimestampType;
import com.ververica.cdc.common.types.MapType;
import com.ververica.cdc.common.types.RowType;
import com.ververica.cdc.common.types.SmallIntType;
import com.ververica.cdc.common.types.TimestampType;
import com.ververica.cdc.common.types.TinyIntType;
import com.ververica.cdc.common.types.VarBinaryType;
import com.ververica.cdc.common.types.VarCharType;
import com.ververica.cdc.common.types.ZonedTimestampType;
import org.apache.doris.flink.catalog.doris.DorisType;

import static org.apache.doris.flink.catalog.doris.DorisType.BIGINT;
import static org.apache.doris.flink.catalog.doris.DorisType.BOOLEAN;
import static org.apache.doris.flink.catalog.doris.DorisType.DATE_V2;
import static org.apache.doris.flink.catalog.doris.DorisType.DOUBLE;
import static org.apache.doris.flink.catalog.doris.DorisType.FLOAT;
import static org.apache.doris.flink.catalog.doris.DorisType.INT;
import static org.apache.doris.flink.catalog.doris.DorisType.SMALLINT;
import static org.apache.doris.flink.catalog.doris.DorisType.STRING;
import static org.apache.doris.flink.catalog.doris.DorisType.TINYINT;
import static org.apache.doris.flink.catalog.doris.DorisType.VARCHAR;

/** Mapping classes of cdc type and doris type. */
public class DorisTypeMapper {

    /** Max size of char type of Doris. */
    public static final int MAX_CHAR_SIZE = 255;

    /** Max size of varchar type of Doris. */
    public static final int MAX_VARCHAR_SIZE = 65533;

    public static String toDorisType(DataType type) {
        return type.accept(new LogicalTypeVisitor());
    }

    private static class LogicalTypeVisitor extends DataTypeDefaultVisitor<String> {

        @Override
        public String visit(CharType charType) {
            // In Doris, strings are stored in UTF-8 encoding, so English characters occupy 1 byte
            // and Chinese characters occupy 3 bytes. The length here is multiplied by 3. The
            // maximum length of CHAR is 255. Once exceeded, it will automatically be converted to
            // VARCHAR type.
            long length = charType.getLength() * 3L;
            if (length <= MAX_CHAR_SIZE) {
                return String.format("%s(%s)", DorisType.CHAR, length);
            } else {
                return visit(new VarCharType(charType.getLength()));
            }
        }

        @Override
        public String visit(VarCharType varCharType) {
            long length = varCharType.getLength() * 3L;
            return length >= MAX_VARCHAR_SIZE ? STRING : String.format("%s(%s)", VARCHAR, length);
        }

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            return STRING;
        }

        @Override
        public String visit(DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            return precision <= 38
                    ? String.format(
                            "%s(%s,%s)", DorisType.DECIMAL_V3, precision, Math.max(scale, 0))
                    : DorisType.STRING;
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

        @Override
        public String visit(DateType dateType) {
            return DATE_V2;
        }

        @Override
        public String visit(TimestampType timestampType) {
            int precision = DataTypeChecks.getPrecision(timestampType);
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(ZonedTimestampType timestampType) {
            int precision = DataTypeChecks.getPrecision(timestampType);
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(LocalZonedTimestampType timestampType) {
            int precision = DataTypeChecks.getPrecision(timestampType);
            return String.format(
                    "%s(%s)", DorisType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
        }

        @Override
        public String visit(ArrayType arrayType) {
            return STRING;
        }

        @Override
        public String visit(MapType mapType) {
            return STRING;
        }

        @Override
        public String visit(RowType rowType) {
            return STRING;
        }

        @Override
        protected String defaultMethod(DataType dataType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Flink doesn't support converting type %s to Doris type yet.",
                            dataType.toString()));
        }
    }
}
