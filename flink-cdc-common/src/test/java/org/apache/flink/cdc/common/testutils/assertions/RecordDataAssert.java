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

package org.apache.flink.cdc.common.testutils.assertions;

import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.schema.Schema;

import org.assertj.core.api.AbstractAssert;

/** Assertions for {@link RecordData}. */
public class RecordDataAssert<SELF extends RecordDataAssert<SELF>>
        extends AbstractAssert<SELF, RecordData> {

    public static <SELF extends RecordDataAssert<SELF>> RecordDataAssert<SELF> assertThatRecordData(
            RecordData recordData) {
        return new RecordDataAssert<>(recordData, RecordDataAssert.class);
    }

    public RecordDataAssert(RecordData recordData, Class<?> selfType) {
        super(recordData, selfType);
    }

    public RecordDataWithSchemaAssert withSchema(Schema schema) {
        return new RecordDataWithSchemaAssert(actual, schema);
    }

    public SELF hasArity(int arity) {
        if (actual.getArity() != arity) {
            failWithActualExpectedAndMessage(
                    actual.getArity(), arity, "The RecordData has unexpected arity");
        }
        return myself;
    }
}
