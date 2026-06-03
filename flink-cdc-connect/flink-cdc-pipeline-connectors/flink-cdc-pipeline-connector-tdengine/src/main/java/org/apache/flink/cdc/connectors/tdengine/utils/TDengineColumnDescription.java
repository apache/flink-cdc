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

package org.apache.flink.cdc.connectors.tdengine.utils;

import java.util.Objects;

/** One column entry returned by TDengine {@code DESCRIBE stable}. */
public final class TDengineColumnDescription {

    private final String name;
    private final String type;
    private final boolean tag;

    public TDengineColumnDescription(String name, String type, boolean tag) {
        this.name = name;
        this.type = type;
        this.tag = tag;
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public boolean isTag() {
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TDengineColumnDescription that = (TDengineColumnDescription) o;
        return tag == that.tag
                && Objects.equals(name, that.name)
                && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, tag);
    }
}
