package org.apache.flink.cdc.connectors.tidb.source.offset;

import org.apache.flink.cdc.connectors.base.source.meta.offset.Offset;
import org.apache.flink.cdc.connectors.base.source.meta.offset.OffsetFactory;

import java.util.Map;

public class LogMessageOffsetFactory extends OffsetFactory {

  @Override
  public Offset newOffset(Map<String, String> offset) {
    return null;
  }

  @Override
  public Offset newOffset(String filename, Long position) {
    return null;
  }

  @Override
  public Offset newOffset(Long position) {
    return null;
  }

  @Override
  public Offset createTimestampOffset(long timestampMillis) {
    return null;
  }

  @Override
  public Offset createInitialOffset() {
    return null;
  }

  @Override
  public Offset createNoStoppingOffset() {
    return null;
  }
}
