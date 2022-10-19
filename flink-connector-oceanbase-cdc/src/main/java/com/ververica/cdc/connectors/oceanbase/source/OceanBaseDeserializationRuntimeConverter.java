/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.oceanbase.source;

import com.oceanbase.oms.logmessage.ByteString;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

/**
 * Runtime converter that converts objects of OceanBase into objects of Flink Table & SQL internal
 * data structures.
 */
public interface OceanBaseDeserializationRuntimeConverter extends Serializable {

    default Object convert(Object object) throws Exception {
        if (object instanceof ByteString) {
            return convertChangeEvent(
                    ((ByteString) object).toString(StandardCharsets.UTF_8.name()));
        } else {
            return convertSnapshotEvent(object);
        }
    }

    default Object convertSnapshotEvent(Object object) throws Exception {
        throw new NotImplementedException();
    }

    default Object convertChangeEvent(String string) throws Exception {
        throw new NotImplementedException();
    }
}
