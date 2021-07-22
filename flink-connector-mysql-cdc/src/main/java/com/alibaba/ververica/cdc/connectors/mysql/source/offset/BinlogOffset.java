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

package com.alibaba.ververica.cdc.connectors.mysql.source.offset;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

/** A structure describes an offset in a binlog of MySQL server. */
public class BinlogOffset implements Comparable<BinlogOffset>, Serializable {

    public static final BinlogOffset INITIAL_OFFSET = new BinlogOffset("", 0);
    private final String filename;
    private final long position;

    public BinlogOffset(String filename, long position) {
        Preconditions.checkNotNull(filename);
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
    public int compareTo(BinlogOffset o) {
        if (this.filename.equals(o.filename)) {
            return Long.compare(this.position, o.position);
        } else {
            // The bing log filenames are ordered
            return this.getFilename().compareTo(o.getFilename());
        }
    }

    public boolean isAtOrBefore(BinlogOffset that) {
        return this.compareTo(that) >= 0;
    }

    @Override
    public String toString() {
        return filename + ":" + position;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BinlogOffset that = (BinlogOffset) o;
        return position == that.position && Objects.equals(filename, that.filename);
    }

    @Override
    public int hashCode() {
        return Objects.hash(filename, position);
    }
}
