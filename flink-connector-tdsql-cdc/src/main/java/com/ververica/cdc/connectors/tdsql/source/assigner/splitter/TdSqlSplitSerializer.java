package com.ververica.cdc.connectors.tdsql.source.assigner.splitter;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplitSerializer;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;

import java.io.IOException;

/** A serializer for the {@link TdSqlSplit}. */
public class TdSqlSplitSerializer implements SimpleVersionedSerializer<TdSqlSplit> {
    public static final TdSqlSplitSerializer INSTANCE = new TdSqlSplitSerializer();

    private static final ThreadLocal<DataOutputSerializer> SERIALIZER_CACHE =
            ThreadLocal.withInitial(() -> new DataOutputSerializer(64));

    private final MySqlSplitSerializer mySqlSplitSerializer;

    public TdSqlSplitSerializer() {
        this.mySqlSplitSerializer = MySqlSplitSerializer.INSTANCE;
    }

    public MySqlSplitSerializer mySqlSplitSerializer() {
        return this.mySqlSplitSerializer;
    }

    @Override
    public int getVersion() {
        return mySqlSplitSerializer.getVersion();
    }

    @Override
    public byte[] serialize(TdSqlSplit split) throws IOException {
        if (split.serializedFormCache != null) {
            return split.serializedFormCache;
        }
        final DataOutputSerializer out = SERIALIZER_CACHE.get();
        serializeTdSqlSet(split.setInfo(), out);

        final byte[] mySqlSeri = mySqlSplitSerializer.serialize(split.mySqlSplit());
        out.writeInt(mySqlSeri.length);
        out.write(mySqlSeri);

        final byte[] result = out.getCopyOfBuffer();
        out.clear();

        split.serializedFormCache = result;

        return result;
    }

    public void serializeTdSqlSet(TdSqlSet set, DataOutputSerializer out) throws IOException {
        out.writeUTF(set.getSetKey());
        out.writeUTF(set.getHost());
        out.writeInt(set.getPort());
    }

    public TdSqlSet deserializeTdSqlSet(final DataInputDeserializer in) throws IOException {
        String setKey = in.readUTF();
        String setHost = in.readUTF();
        int port = in.readInt();

        return new TdSqlSet(setKey, setHost, port);
    }

    @Override
    public TdSqlSplit deserialize(int version, byte[] serialized) throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);

        TdSqlSet set = deserializeTdSqlSet(in);

        int mySqlSerLen = in.readInt();

        byte[] mySqlSeri = new byte[mySqlSerLen];
        in.read(mySqlSeri);
        MySqlSplit mySqlSplit = mySqlSplitSerializer.deserialize(version, mySqlSeri);

        in.releaseArrays();

        return new TdSqlSplit(set, mySqlSplit);
    }
}
