/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.fluss.sink.v2.metrics;

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;

/* This file is based on source code of Apache Fluss Project (https://fluss.apache.org/), licensed by the Apache
 * Software Foundation (ASF) under the Apache License, Version 2.0. See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership. */

/** An implementation of Flink's {@link Histogram} which wraps Fluss's Histogram. */
public class WrapperFlussHistogram implements Histogram {

    private final com.alibaba.fluss.metrics.Histogram flussHistogram;

    public WrapperFlussHistogram(com.alibaba.fluss.metrics.Histogram flussHistogram) {
        this.flussHistogram = flussHistogram;
    }

    @Override
    public void update(long n) {
        flussHistogram.update(n);
    }

    @Override
    public long getCount() {
        return flussHistogram.getCount();
    }

    @Override
    public HistogramStatistics getStatistics() {

        flussHistogram.getStatistics();

        return null;
    }

    private static class FlinkHistogramStatistics extends HistogramStatistics {

        private final com.alibaba.fluss.metrics.HistogramStatistics flussHistogramStatistics;

        public FlinkHistogramStatistics(
                com.alibaba.fluss.metrics.HistogramStatistics flussHistogramStatistics) {
            this.flussHistogramStatistics = flussHistogramStatistics;
        }

        @Override
        public double getQuantile(double quantile) {
            return flussHistogramStatistics.getQuantile(quantile);
        }

        @Override
        public long[] getValues() {
            return flussHistogramStatistics.getValues();
        }

        @Override
        public int size() {
            return flussHistogramStatistics.size();
        }

        @Override
        public double getMean() {
            return flussHistogramStatistics.getMean();
        }

        @Override
        public double getStdDev() {
            return flussHistogramStatistics.getStdDev();
        }

        @Override
        public long getMax() {
            return flussHistogramStatistics.getMax();
        }

        @Override
        public long getMin() {
            return flussHistogramStatistics.getMin();
        }
    }
}
