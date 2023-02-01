package org.tikv.common.codec;

import org.tikv.common.exception.CodecException;
import org.tikv.common.meta.TiColumnInfo;
import org.tikv.common.meta.TiTableInfo;
import org.tikv.common.row.Row;

import java.util.List;

/**
 * Copied from https://github.com/tikv/client-java project to fix
 */
public class TableCodec {
    public static byte[] encodeRow(
            List<TiColumnInfo> columnInfos,
            Object[] values,
            boolean isPkHandle,
            boolean encodeWithNewRowFormat)
            throws IllegalAccessException {
        if (columnInfos.size() != values.length) {
            throw new IllegalAccessException(
                    String.format(
                            "encodeRow error: data and columnID count not " + "match %d vs %d",
                            columnInfos.size(), values.length));
        }
        if (encodeWithNewRowFormat) {
            return TableCodecV2.encodeRow(columnInfos, values, isPkHandle);
        }
        return TableCodecV1.encodeRow(columnInfos, values, isPkHandle);
    }

    public static Object[] decodeObjects(byte[] value, Long handle, TiTableInfo tableInfo) {
        if (value.length == 0) {
            throw new CodecException("Decode fails: value length is zero");
        }
        if ((value[0] & 0xff) == RowV2.CODEC_VER) {
            return TableCodecV2.decodeObjects(value, handle, tableInfo);
        }
        return TableCodecV1.decodeObjects(value, handle, tableInfo);
    }

    public static Row decodeRow(byte[] value, Long handle, TiTableInfo tableInfo) {
        if (value.length == 0) {
            throw new CodecException("Decode fails: value length is zero");
        }
        if ((value[0] & 0xff) == RowV2.CODEC_VER) {
            return TableCodecV2.decodeRow(value, handle, tableInfo);
        }
        return TableCodecV1.decodeRow(value, handle, tableInfo);
    }

    public static long decodeHandle(byte[] value) {
        return new CodecDataInput(value).readLong();
    }
}
