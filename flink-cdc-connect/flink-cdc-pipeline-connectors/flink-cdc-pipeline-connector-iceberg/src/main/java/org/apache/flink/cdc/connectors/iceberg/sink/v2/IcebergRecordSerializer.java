package org.apache.flink.cdc.connectors.iceberg.sink.v2;

import java.io.IOException;
import java.io.Serializable;

public interface IcebergRecordSerializer<Input> extends Serializable {
    IcebergEvent serialize(Input t) throws IOException;
}
