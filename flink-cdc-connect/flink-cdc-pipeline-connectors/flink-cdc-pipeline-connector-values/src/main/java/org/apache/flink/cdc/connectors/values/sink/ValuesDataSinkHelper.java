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

package org.apache.flink.cdc.connectors.values.sink;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;

import org.apache.flink.shaded.guava31.com.google.common.io.BaseEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** A helper class for {@link ValuesDataSink} to process {@link Event}. */
public class ValuesDataSinkHelper {

    /** convert Event to String for {@link ValuesDataSink} to display detail message. */
    public static String convertEventToStr(Event event, List<RecordData.FieldGetter> fieldGetters) {
        if (event instanceof SchemaChangeEvent) {
            return event.toString();
        } else if (event instanceof DataChangeEvent) {
            DataChangeEvent dataChangeEvent = (DataChangeEvent) event;
            String eventStr =
                    "DataChangeEvent{"
                            + "tableId="
                            + dataChangeEvent.tableId()
                            + ", before="
                            + getFields(fieldGetters, dataChangeEvent.before())
                            + ", after="
                            + getFields(fieldGetters, dataChangeEvent.after())
                            + ", op="
                            + dataChangeEvent.op()
                            + ", meta="
                            + dataChangeEvent.describeMeta()
                            + '}';
            return eventStr;
        }
        return "Event{}";
    }

    private static List<Object> getFields(
            List<RecordData.FieldGetter> fieldGetters, RecordData recordData) {
        List<Object> fields = new ArrayList<>(fieldGetters.size());
        if (recordData == null) {
            return fields;
        }
        for (RecordData.FieldGetter fieldGetter : fieldGetters) {
            fields.add(fieldGetter.getFieldOrNull(recordData));
        }
        return fields.stream()
                .map(
                        o -> {
                            if (o == null) {
                                return "null";
                            }
                            if (o instanceof byte[]) {
                                return BaseEncoding.base64().encode((byte[]) o);
                            } else {
                                return o.toString();
                            }
                        })
                .collect(Collectors.toList());
    }
}
