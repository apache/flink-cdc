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

package com.ververica.cdc.connectors.tdsql.testutils;

/** MySql version enum. */
public enum MySqlVersion {
    V8_0("mysql", "8.0"),
    AARACH64_V8_0("arm64v8/mysql", "8.0-oracle");

    private String version;
    private String image;

    MySqlVersion(String image, String version) {
        this.version = version;
        this.image = image;
    }

    public String getVersion() {
        return version;
    }

    public String getImage() {
        return image;
    }

    @Override
    public String toString() {
        return "MySqlVersion{" + "version='" + version + '\'' + '}';
    }
}
