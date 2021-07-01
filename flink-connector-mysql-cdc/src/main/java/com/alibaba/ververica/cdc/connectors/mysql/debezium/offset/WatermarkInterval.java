/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.debezium.offset;

import java.io.Serializable;

/**
 * The watermark interval used to describe the start point and end point of a split scan. This is
 * inspired by https://arxiv.org/pdf/2010.12597v1.pdf.
 */
public class WatermarkInterval implements Serializable {

    private BinlogPosition lowWatermark;
    private BinlogPosition highWatermark;

    public WatermarkInterval(BinlogPosition lowWatermark, BinlogPosition highWatermark) {
        this.lowWatermark = lowWatermark;
        this.highWatermark = highWatermark;
    }

    public BinlogPosition getLowWatermark() {
        return lowWatermark;
    }

    public void setLowWatermark(BinlogPosition lowWatermark) {
        this.lowWatermark = lowWatermark;
    }

    public BinlogPosition getHighWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(BinlogPosition highWatermark) {
        this.highWatermark = highWatermark;
    }
}
