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

package org.apache.flink.cdc.connectors.kafka.json.canal;

/** Canal JSON struct. */
public class CanalJsonStruct {

    enum CanalStruct {
        OLD(0, "old"),
        DATA(1, "data"),
        TYPE(2, "type"),
        DATABASE(3, "database"),
        TABLE(4, "table"),
        PK_NAMES(5, "pkNames");

        private final int position;
        private final String fieldName;

        CanalStruct(int position, String fieldName) {
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

    enum CanalMeta {
        MYSQL_TYPE(0, "mysqlType"),
        IS_DDL(1, "isDdl"),
        TS(2, "ts");

        private final int position;
        private final String fieldName;

        CanalMeta(int position, String fieldName) {
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

    enum CanalOperation {
        INSERT("INSERT"),
        UPDATE("UPDATE"),
        DELETE("DELETE");

        private final String fieldName;

        CanalOperation(String fieldName) {
            this.fieldName = fieldName;
        }

        public String getFieldName() {
            return fieldName;
        }

        static CanalOperation fromFieldName(String fieldName) {
            for (CanalOperation operation : values()) {
                if (operation.fieldName.equalsIgnoreCase(fieldName)) {
                    return operation;
                }
            }
            return null;
        }
    }
}
