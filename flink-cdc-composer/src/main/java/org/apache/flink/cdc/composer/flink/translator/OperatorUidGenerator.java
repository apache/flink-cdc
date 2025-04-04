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

package org.apache.flink.cdc.composer.flink.translator;

import javax.annotation.Nullable;

/**
 * A generator for pipeline operator UIDs. Generates UIDs using a configured prefix and an
 * operator-specific suffix.
 *
 * <p>If the prefix is null, it returns null regardless of the provided suffix.
 */
public class OperatorUidGenerator {

    private final @Nullable String prefix;

    public OperatorUidGenerator(@Nullable String prefix) {
        if (prefix != null && prefix.isEmpty()) {
            throw new IllegalArgumentException(
                    "The prefix can be null but cannot be an empty string.");
        }
        this.prefix = prefix;
    }

    public OperatorUidGenerator() {
        this(null);
    }

    public @Nullable String generateUid(String suffix) {
        if (prefix == null) {
            return null;
        }
        return String.format("%s-%s", prefix, suffix);
    }
}
