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

package org.apache.flink.cdc.runtime.operators.transform;

import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * a Pojo class to describe the information of the primaryKeys/partitionKeys/options transformation
 * of {@link Schema}.
 */
public class SchemaMetadataTransform implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> primaryKeys = new ArrayList<>();

    private List<String> partitionKeys = new ArrayList<>();

    private Map<String, String> options = new HashMap<>();

    public SchemaMetadataTransform(
            String primaryKeyString, String partitionKeyString, String tableOptionString) {
        if (!StringUtils.isNullOrWhitespaceOnly(primaryKeyString)) {
            String[] primaryKeyArr = primaryKeyString.split(",");
            for (int i = 0; i < primaryKeyArr.length; i++) {
                primaryKeyArr[i] = primaryKeyArr[i].trim();
            }
            primaryKeys = Arrays.asList(primaryKeyArr);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(partitionKeyString)) {
            String[] partitionKeyArr = partitionKeyString.split(",");
            for (int i = 0; i < partitionKeyArr.length; i++) {
                partitionKeyArr[i] = partitionKeyArr[i].trim();
            }
            partitionKeys = Arrays.asList(partitionKeyArr);
        }
        if (!StringUtils.isNullOrWhitespaceOnly(tableOptionString)) {
            for (String tableOption : tableOptionString.split(",")) {
                String[] kv = tableOption.split("=");
                if (kv.length != 2) {
                    throw new IllegalArgumentException(
                            "table option format error: "
                                    + tableOptionString
                                    + ", it should be like `key1=value1,key2=value2`.");
                }
                options.put(kv[0].trim(), kv[1].trim());
            }
        }
    }

    public List<String> getPrimaryKeys() {
        return primaryKeys;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public Map<String, String> getOptions() {
        return options;
    }
}
