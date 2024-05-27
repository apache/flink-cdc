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

package org.apache.flink.cdc.connectors.maxcompute.common;

/** Constant use for MaxCompute Connector. */
public class Constant {
    public static final String TUNNEL_SESSION_ID = "tunnel_session_id";
    public static final String MAXCOMPUTE_PARTITION_NAME = "maxcompute_partition_name";
    public static final String SCHEMA_ENABLE_FLAG = "odps.schema.model.enabled";

    public static final String PIPELINE_SESSION_MANAGE_OPERATOR_UID =
            "$$_session_manage_operator_$$";

    public static final String END_OF_SESSION = "end_of_session";
}
