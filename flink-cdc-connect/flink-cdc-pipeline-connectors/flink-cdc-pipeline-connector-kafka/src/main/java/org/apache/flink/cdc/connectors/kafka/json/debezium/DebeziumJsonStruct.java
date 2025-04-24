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

package org.apache.flink.cdc.connectors.kafka.json.debezium;

/** Debezium JSON struct. */
public class DebeziumJsonStruct {

    enum DebeziumStruct {
        SCHEMA(0, "schema"),
        PAYLOAD(1, "payload");

        private final int position;
        private final String fieldName;

        DebeziumStruct(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    enum DebeziumPayload {
        BEFORE(0, "before"),
        AFTER(1, "after"),
        OPERATION(2, "op"),
        SOURCE(3, "source");

        private final int position;
        private final String fieldName;

        DebeziumPayload(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }

    enum DebeziumSource {
        DATABASE(0, "db"),
        TABLE(1, "table");

        private final int position;
        private final String fieldName;

        DebeziumSource(int position, String fieldName) {
            this.position = position;
            this.fieldName = fieldName;
        }

        public int getPosition() {
            return position;
        }

        public String getFieldName() {
            return fieldName;
        }
    }
}
