package com.ververica.cdc.debezium;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ververica.cdc.debezium.utils.SourceRecordUtil;
import javafx.util.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;

import java.util.HashMap;
import java.util.Map;


public class PairDebeziumDeserializationSchema implements DebeziumDeserializationSchema<Pair> {

    private transient ObjectMapper objectMapper;
    private transient JsonConverter valueConverter;
    private transient JsonConverter keyConverter;
    private transient boolean initialized;
    private final int timeZoneOffset;

    public PairDebeziumDeserializationSchema() {
        this.timeZoneOffset = 8;
    }

    public PairDebeziumDeserializationSchema(int timeZoneOffset) {
        this.timeZoneOffset = timeZoneOffset;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<Pair> collector) throws Exception {
        if (!initialized) {
            initialize();
        }

        SourceRecord sourceRecord = SourceRecordUtil.correctTimeZoneSourceRecord(record, timeZoneOffset);

        byte[] value = valueConverter.fromConnectData(
                sourceRecord.topic(), sourceRecord.valueSchema(), sourceRecord.value());

        if (sourceRecord.key() == null) {
            collector.collect(new Pair(null, objectMapper.readTree(value)));
        } else {
            byte[] key =
                    keyConverter.fromConnectData(
                            sourceRecord.topic(), sourceRecord.keySchema(), sourceRecord.key());
            collector.collect(new Pair(objectMapper.readTree(key), objectMapper.readTree(value)));
        }
    }

    private void initialize() {
        initialized = true;
        objectMapper = new ObjectMapper();

        valueConverter = new JsonConverter();
        Map<String, Object> valueConfigs = new HashMap<>(2);
        valueConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
        valueConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        valueConverter.configure(valueConfigs);

        keyConverter = new JsonConverter();
        Map<String, Object> keyConfigs = new HashMap<>(2);
        keyConfigs.put(ConverterConfig.TYPE_CONFIG, ConverterType.KEY.getName());
        keyConfigs.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, false);
        keyConverter.configure(keyConfigs);
    }

    @Override
    public TypeInformation<Pair> getProducedType() {
        return TypeInformation.of(Pair.class);
    }
}
