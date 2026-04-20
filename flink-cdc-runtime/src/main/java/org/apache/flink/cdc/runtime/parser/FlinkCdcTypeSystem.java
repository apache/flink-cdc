/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.runtime.parser;

import org.apache.flink.cdc.common.pipeline.DecimalPrecisionMode;

import org.apache.calcite.rel.type.RelDataTypeSystemImpl;

/** A customized version of {@link org.apache.calcite.rel.type.RelDataTypeSystem}. */
public class FlinkCdcTypeSystem extends RelDataTypeSystemImpl {

    public static final FlinkCdcTypeSystem UP_TO_38 = new FlinkCdcTypeSystem(38);
    public static final FlinkCdcTypeSystem UP_TO_19 = new FlinkCdcTypeSystem(19);

    private final int maxPrecision;

    private FlinkCdcTypeSystem(int maxPrecision) {
        this.maxPrecision = maxPrecision;
    }

    public static FlinkCdcTypeSystem of(DecimalPrecisionMode mode) {
        switch (mode) {
            case UP_TO_38:
                return UP_TO_38;
            case UP_TO_19:
                return UP_TO_19;
            default:
                throw new IllegalArgumentException("Unexpected decimal precision mode: " + mode);
        }
    }

    @Override
    public int getMaxNumericPrecision() {
        return maxPrecision;
    }

    @Override
    public int getMaxNumericScale() {
        return maxPrecision;
    }
}
