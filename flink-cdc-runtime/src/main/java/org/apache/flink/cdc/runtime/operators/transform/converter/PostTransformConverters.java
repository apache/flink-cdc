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

package org.apache.flink.cdc.runtime.operators.transform.converter;

import org.apache.flink.cdc.common.annotation.Experimental;
import org.apache.flink.cdc.common.utils.StringUtils;

import java.util.Optional;

/** The {@link PostTransformConverter} utils. */
@Experimental
public class PostTransformConverters {
    public static final String SOFT_DELETE_CONVERTER = "SOFT_DELETE";

    /** Get the {@link PostTransformConverter} by given identifier. */
    public static Optional<PostTransformConverter> of(String identifier) {
        if (StringUtils.isNullOrWhitespaceOnly(identifier)) {
            return Optional.empty();
        }

        switch (identifier) {
            case SOFT_DELETE_CONVERTER:
                return Optional.of(new SoftDeleteConverter());
            default:
                throw new IllegalArgumentException(
                        String.format("Failed to find the converter %s.", identifier));
        }
    }
}
