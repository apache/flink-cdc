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

package org.apache.flink.cdc.cli.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/** Base64 util for {@link Base64Util}. */
public class Base64Util {
    private static Base64 base64;
    private static Base64 base64Safe;

    static {
        base64 = new Base64();
        base64Safe = new Base64(true);
    }

    public static void main(String[] args) throws IOException {
        FileUtils.writeStringToFile(
                new File(args[1]),
                encode(
                        FileUtils.readFileToString(
                                new File(args[0]), StandardCharsets.UTF_8.name())),
                StandardCharsets.UTF_8.name());
    }

    public static String encode(String value) {
        return base64.encodeToString(value.getBytes());
    }

    public static String encodeSafe(String value) {
        return base64Safe.encodeToString(value.getBytes());
    }

    public static String decode(String value) {
        return new String(base64.decode(value));
    }

    public static String decodeSafe(String value) {
        return new String(base64Safe.decode(value));
    }
}
