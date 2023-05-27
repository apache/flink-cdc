package com.ververica.cdc.connectors.oceanbase.source.metrics;

import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;

public class OceanBaseMetrics {
  private final MetricGroup metricGroup;

  /**
   * The last record processing time, which is updated after {@link OceanBaseMetrics} fetches a
   * batch of data. It's mainly used to report metrics sourceIdleTime for sourceIdleTime =
   * System.currentTimeMillis() - processTime.
   */
  private volatile long processTime = 0L;

  /**
   * currentFetchEventTimeLag = FetchTime - messageTimestamp, where the FetchTime is the time the
   * record fetched into the source operator.
   */
  private volatile long fetchDelay = 0L;

  /**
   * emitDelay = EmitTime - messageTimestamp, where the EmitTime is the time the record leaves the
   * source operator.
   */
  private volatile long emitDelay = 0L;

  public OceanBaseMetrics(MetricGroup metricGroup) {
    this.metricGroup = metricGroup;
  }

  public void registerMetrics() {
    metricGroup.gauge("currentFetchEventTimeLag", (Gauge<Long>) this::getFetchDelay);
    metricGroup.gauge("currentEmitEventTimeLag", (Gauge<Long>) this::getEmitDelay);
    metricGroup.gauge("sourceIdleTime", (Gauge<Long>) this::getIdleTime);
  }

  public long getFetchDelay() {
    return fetchDelay;
  }

  public long getEmitDelay() {
    return emitDelay;
  }

  public long getIdleTime() {
    // no previous process time at the beginning, return 0 as idle time
    if (processTime == 0) {
      return 0;
    }
    return System.currentTimeMillis() - processTime;
  }

  public void recordProcessTime(long processTime) {
    this.processTime = processTime;
  }

  public void recordFetchDelay(long fetchDelay) {
    this.fetchDelay = fetchDelay;
  }

  public void recordEmitDelay(long emitDelay) {
    this.emitDelay = emitDelay;
  }
}
