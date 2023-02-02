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

package org.tikv.common.codec;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.tikv.common.ExtendedDateTime;
import org.tikv.common.exception.CodecException;
import org.tikv.common.exception.TypeException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.types.Converter;
import org.tikv.common.types.DataType;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Copied from https://github.com/tikv/client-java project to fix */
public class RowEncoderV2 {
    private static final long SIGN_MASK = 0x8000000000000000L;
    private int numCols;
    private Object[] values;
    private RowV2 row;

    public RowEncoderV2() {}

    public byte[] encode(List<TiColumnInfo> columnInfos, List<Object> values) {
        this.row = RowV2.createEmpty();
        numCols = columnInfos.size();
        for (int i = 0; i < numCols; i++) {
            if (columnInfos.get(i).getId() > 255) {
                this.row.large = true;
            }
            if (values.get(i) == null) {
                this.row.numNullCols++;
            } else {
                this.row.numNotNullCols++;
            }
        }

        this.values = new Object[numCols];
        reformatCols(columnInfos, values);
        encodeRowCols(columnInfos);
        return this.row.toBytes();
    }

    private void reformatCols(List<TiColumnInfo> columnInfos, List<Object> valueList) {
        int nullIdx = numCols - row.numNullCols;
        int notNullIdx = 0;
        if (this.row.large) {
            row.initColIDs32();
            row.initOffsets32();
        } else {
            row.initColIDs();
            row.initOffsets();
        }
        for (int i = 0; i < numCols; i++) {
            int colID = (int) columnInfos.get(i).getId();
            Object value = valueList.get(i);
            if (value == null) {
                if (this.row.large) {
                    this.row.colIDs32[nullIdx] = colID;
                } else {
                    this.row.colIDs[nullIdx] = (byte) colID;
                }
                nullIdx++;
            } else {
                if (this.row.large) {
                    this.row.colIDs32[notNullIdx] = colID;
                } else {
                    this.row.colIDs[notNullIdx] = (byte) colID;
                }
                valueList.set(notNullIdx, value);
                notNullIdx++;
            }
        }
        // sort colIDs together with corresponding values
        int len = this.row.numNotNullCols;
        if (this.row.large) {
            int[] temp = Arrays.copyOfRange(this.row.colIDs32, 0, len);
            Integer[] idx = new Integer[len];
            for (int i = 0; i < len; i++) {
                idx[i] = i;
            }
            Arrays.sort(idx, Comparator.comparingInt(o -> this.row.colIDs32[o]));
            for (int i = 0; i < len; i++) {
                this.row.colIDs32[i] = temp[idx[i]];
                this.values[i] = valueList.get(idx[i]);
            }
            if (this.row.numNullCols > 0) {
                len = this.row.numNullCols;
                int start = this.row.numNotNullCols;
                temp = Arrays.copyOfRange(this.row.colIDs32, start, start + len);
                idx = new Integer[len];
                for (int i = 0; i < len; i++) {
                    idx[i] = i;
                }
                Arrays.sort(idx, Comparator.comparingInt(o -> this.row.colIDs32[start + o]));
                for (int i = 0; i < len; i++) {
                    // values should all be null
                    this.row.colIDs32[start + i] = temp[idx[i]];
                }
            }
        } else {
            byte[] temp = Arrays.copyOfRange(this.row.colIDs, 0, len);
            Integer[] idx = new Integer[len];
            for (int i = 0; i < len; i++) {
                idx[i] = i;
            }
            Arrays.sort(idx, Comparator.comparingInt(o -> this.row.colIDs[o]));
            for (int i = 0; i < len; i++) {
                this.row.colIDs[i] = temp[idx[i]];
                this.values[i] = valueList.get(idx[i]);
            }
            if (this.row.numNullCols > 0) {
                len = this.row.numNullCols;
                int start = this.row.numNotNullCols;
                temp = Arrays.copyOfRange(this.row.colIDs, start, start + len);
                idx = new Integer[len];
                for (int i = 0; i < len; i++) {
                    idx[i] = i;
                }
                Arrays.sort(idx, Comparator.comparingInt(o -> this.row.colIDs[start + o]));
                for (int i = 0; i < len; i++) {
                    // values should all be null
                    this.row.colIDs[start + i] = temp[idx[i]];
                }
            }
        }
    }

    private TiColumnInfo getColumnInfoByID(List<TiColumnInfo> columnInfos, int id) {
        for (TiColumnInfo columnInfo : columnInfos) {
            if (columnInfo.getId() == id) {
                return columnInfo;
            }
        }
        throw new CodecException("column id " + id + " not found in ColumnInfo");
    }

    private void encodeRowCols(List<TiColumnInfo> columnInfos) {
        CodecDataOutputLittleEndian cdo = new CodecDataOutputLittleEndian();
        for (int i = 0; i < this.row.numNotNullCols; i++) {
            Object o = this.values[i];
            if (this.row.large) {
                encodeValue(cdo, o, getColumnInfoByID(columnInfos, this.row.colIDs32[i]).getType());
            } else {
                encodeValue(cdo, o, getColumnInfoByID(columnInfos, this.row.colIDs[i]).getType());
            }
            if (cdo.size() > 0xffff && !this.row.large) {
                // only initialize once
                this.row.initColIDs32();
                for (int j = 0; j < numCols; j++) {
                    this.row.colIDs32[j] = this.row.colIDs[j];
                }
                this.row.initOffsets32();
                if (numCols >= 0) {
                    System.arraycopy(this.row.offsets, 0, this.row.offsets32, 0, numCols);
                }
                this.row.large = true;
            }
            if (this.row.large) {
                this.row.offsets32[i] = cdo.size();
            } else {
                this.row.offsets[i] = cdo.size();
            }
        }
        this.row.data = cdo.toBytes();
    }

    private void encodeValue(CodecDataOutput cdo, Object value, DataType tp) {
        switch (tp.getType()) {
            case TypeLonglong:
            case TypeLong:
            case TypeInt24:
            case TypeShort:
            case TypeTiny:
                // TODO: encode consider unsigned
                encodeInt(cdo, (long) value);
                break;
            case TypeFloat:
            case TypeDouble:
                if (value instanceof Double) {
                    encodeDouble(cdo, value);
                } else if (value instanceof Float) {
                    encodeFloat(cdo, value);
                } else {
                    throw new TypeException(
                            "type does not match in encoding, should be float/double");
                }
                break;
            case TypeString:
            case TypeVarString:
            case TypeVarchar:
            case TypeBlob:
            case TypeTinyBlob:
            case TypeMediumBlob:
            case TypeLongBlob:
                encodeString(cdo, value);
                break;
            case TypeNewDecimal:
                encodeDecimal(cdo, value);
                break;
            case TypeBit:
                encodeBit(cdo, value);
                break;
            case TypeTimestamp:
                encodeTimestamp(cdo, value, DateTimeZone.UTC);
                break;
            case TypeDate:
            case TypeDatetime:
                encodeTimestamp(cdo, value, Converter.getLocalTimezone());
                break;
            case TypeDuration:
            case TypeYear:
                encodeInt(cdo, (long) value);
                break;
            case TypeEnum:
                encodeEnum(cdo, value, tp.getElems());
                break;
            case TypeSet:
                encodeSet(cdo, value, tp.getElems());
                break;
            case TypeJSON:
                encodeJson(cdo, value);
                break;
            case TypeNull:
                // ??
            case TypeDecimal:
            case TypeGeometry:
            case TypeNewDate:
                throw new CodecException("type should not appear in encoding");
            default:
                throw new CodecException("invalid data type: " + tp.getType().name());
        }
    }

    private void encodeInt(CodecDataOutput cdo, long value) {
        if (value == (byte) value) {
            cdo.writeByte((byte) value);
        } else if (value == (short) value) {
            cdo.writeShort((short) value);
        } else if (value == (int) value) {
            cdo.writeInt((int) value);
        } else {
            cdo.writeLong(value);
        }
    }

    private void encodeFloat(CodecDataOutput cdo, Object value) {
        long u = Double.doubleToLongBits((float) value);
        if ((float) value >= 0) {
            u |= SIGN_MASK;
        } else {
            u = ~u;
        }
        u = Long.reverseBytes(u);
        cdo.writeLong(u);
    }

    private void encodeDouble(CodecDataOutput cdo, Object value) {
        long u = Double.doubleToLongBits((double) value);
        if ((double) value >= 0) {
            u |= SIGN_MASK;
        } else {
            u = ~u;
        }
        u = Long.reverseBytes(u);
        cdo.writeLong(u);
    }

    private void encodeBit(CodecDataOutput cdo, Object value) {
        long s = 0;
        if (value instanceof Long) {
            s = (long) value;
        } else if (value instanceof byte[]) {
            for (byte b : (byte[]) value) {
                s <<= 8;
                s |= b;
            }
        } else {
            throw new CodecException("invalid bytes type " + value.getClass());
        }
        encodeInt(cdo, s);
    }

    private void encodeTimestamp(CodecDataOutput cdo, Object value, DateTimeZone tz) {
        if (value instanceof Timestamp) {
            Timestamp timestamp = (Timestamp) value;
            DateTime dateTime = new DateTime(timestamp.getTime());
            int nanos = timestamp.getNanos();
            ExtendedDateTime extendedDateTime =
                    new ExtendedDateTime(dateTime, (nanos / 1000) % 1000);
            long t = Codec.DateTimeCodec.toPackedLong(extendedDateTime, tz);
            encodeInt(cdo, t);
        } else if (value instanceof Date) {
            ExtendedDateTime extendedDateTime =
                    new ExtendedDateTime(new DateTime(((Date) value).getTime()));
            long t = Codec.DateTimeCodec.toPackedLong(extendedDateTime, tz);
            encodeInt(cdo, t);
        } else {
            throw new CodecException("invalid timestamp type " + value.getClass());
        }
    }

    private void encodeString(CodecDataOutput cdo, Object value) {
        if (value instanceof byte[]) {
            cdo.write((byte[]) value);
        } else if (value instanceof String) {
            cdo.write(((String) value).getBytes(StandardCharsets.UTF_8));
        } else {
            throw new CodecException("invalid string type " + value.getClass());
        }
    }

    private void encodeDecimal(CodecDataOutput cdo, Object value) {
        if (value instanceof MyDecimal) {
            MyDecimal dec = (MyDecimal) value;
            Codec.DecimalCodec.writeDecimal(cdo, dec, dec.precision(), dec.frac());
        } else if (value instanceof BigDecimal) {
            MyDecimal dec = new MyDecimal();
            BigDecimal decimal = (BigDecimal) value;
            int prec = decimal.precision();
            int frac = decimal.scale();
            dec.fromString(((BigDecimal) value).toPlainString());
            Codec.DecimalCodec.writeDecimal(cdo, dec, prec, frac);
        } else {
            throw new CodecException("invalid decimal type " + value.getClass());
        }
    }

    private void encodeEnum(CodecDataOutput cdo, Object value, List<String> elems) {
        if (value instanceof Integer) {
            encodeInt(cdo, (int) value);
        } else if (value instanceof String) {
            int val = Codec.EnumCodec.parseEnumName((String) value, elems);
            encodeInt(cdo, val);
        } else {
            throw new CodecException("invalid enum type " + value.getClass());
        }
    }

    private void encodeSet(CodecDataOutput cdo, Object value, List<String> elems) {
        // TODO: Support encoding set
        throw new CodecException("Set encoding is not yet supported.");
    }

    private void encodeJson(CodecDataOutput cdo, Object value) {
        // TODO: Support encoding JSON
        throw new CodecException("JSON encoding is not yet supported.");
    }
}
