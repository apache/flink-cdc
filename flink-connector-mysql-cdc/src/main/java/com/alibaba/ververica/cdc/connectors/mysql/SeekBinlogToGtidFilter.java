package com.alibaba.ververica.cdc.connectors.mysql;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link DebeziumDeserializationSchema} which wraps a real {@link DebeziumDeserializationSchema}
 * to seek binlog to the specific gtid.
 */
public class SeekBinlogToGtidFilter<T> implements DebeziumDeserializationSchema<T> {
    private static final long serialVersionUID = -3168848963265670603L;
    protected static final Logger LOG = LoggerFactory.getLogger(SeekBinlogToGtidFilter.class);
    private transient boolean find = false;
    private transient long filtered = 0L;

    private final String gtid;
    private final DebeziumDeserializationSchema<T> serializer;

    public SeekBinlogToGtidFilter(String gtid, DebeziumDeserializationSchema<T> serializer) {
        this.gtid = gtid;
        this.serializer = serializer;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        if (find) {
            serializer.deserialize(record, out);
            return;
        }

        if (filtered == 0) {
            LOG.info("Begin to seek binlog to the specific gtid {}.", gtid);
        }

        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        String sourceGtid = source.getString("gtid");
        if (sourceGtid != null && sourceGtid.equals(gtid)) {
            find = true;
            LOG.info(
                    "Successfully seek to the specific gtid {} with filtered {} change events.",
                    gtid,
                    filtered);
        } else {
            filtered++;
            if (filtered % 10000 == 0) {
                LOG.info(
                        "Seeking binlog to specific gtid with filtered {} change events.",
                        filtered);
            }
        }
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return serializer.getProducedType();
    }
}
