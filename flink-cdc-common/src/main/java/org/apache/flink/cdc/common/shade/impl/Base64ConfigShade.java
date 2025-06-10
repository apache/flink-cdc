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

package org.apache.flink.cdc.common.shade.impl;

import org.apache.flink.cdc.common.shade.ConfigShade;

import java.util.Base64;

/** Base64 ConfigShade. */
public class Base64ConfigShade implements ConfigShade {

    private static final Base64.Decoder DECODER = Base64.getDecoder();

    private static final String IDENTIFIER = "base64";

    @Override
    public String getIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public String decrypt(String content) {
        return new String(DECODER.decode(content));
    }
}
