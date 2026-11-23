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

package org.apache.flink.cdc.common.pipeline;

import org.apache.flink.cdc.common.annotation.PublicEvolving;

/**
 * Maximum precision mode for DECIMAL type in transform expressions. Controls the upper bound of
 * numeric precision used by the SQL type system during expression evaluation.
 */
@PublicEvolving
public enum DecimalPrecisionMode {

    /** Limits DECIMAL precision to 19 digits, matching Calcite's default type system behavior. */
    UP_TO_19,

    /** Allows DECIMAL precision up to 38 digits, matching Flink CDC's extended type system. */
    UP_TO_38
}
