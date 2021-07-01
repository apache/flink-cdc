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

import org.apache.flink.util.Preconditions;

/** A {@link MySQLOffset} implementation that uses binlog position. */
public class BinlogPosition implements MySQLOffset, Comparable<BinlogPosition> {

    public static final BinlogPosition INITIAL_OFFSET = new BinlogPosition("", 0);
    private final String filename;
    private final long position;

    public BinlogPosition(String filename, long position) {
        Preconditions.checkNotNull(filename);
        this.filename = filename;
        this.position = position;
    }

    public BinlogPosition(
            String filename, long position, boolean isLowWatermark, boolean isHighWatermark) {
        this.filename = filename;
        this.position = position;
    }

    public String getFilename() {
        return filename;
    }

    public long getPosition() {
        return position;
    }

    @Override
    public int compareTo(BinlogPosition o) {
        if (this.filename.equals(o.filename)) {
            return Long.compare(this.position, o.position);
        } else {
            // The bing log filenames are ordered
            return this.getFilename().compareTo(o.getFilename());
        }
    }

    public boolean isAtOrBefore(BinlogPosition that) {
        return this.compareTo(that) >= 0;
    }

    @Override
    public String toString() {
        return filename + ":" + position;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + filename.hashCode();
        result = prime * result + (int) (position ^ (position >>> 32));
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        BinlogPosition other = (BinlogPosition) obj;
        if (!filename.equals(other.filename)) {
            return false;
        }
        if (position != other.position) {
            return false;
        }
        return true;
    }
}
