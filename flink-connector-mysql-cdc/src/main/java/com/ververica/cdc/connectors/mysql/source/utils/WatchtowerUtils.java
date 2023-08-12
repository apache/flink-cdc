/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

/** Send notification to watchtower. */
public class WatchtowerUtils {
    private static final Logger LOG = LoggerFactory.getLogger(WatchtowerUtils.class);

    public static void notify(int watchTowerId, String tableName) {
        LOG.info("Send Notification to Watchtower {}", watchTowerId);
        try {
            URL url = new URL("");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setDoInput(true);
            conn.setConnectTimeout(5000);
            conn.setRequestProperty("Content-Type", "application/json");
            String jsonInputString =
                    "{\"to\":\""
                            + watchTowerId
                            + "\",\"msg\":\""
                            + "[SNAPSHOT FINISHED]"
                            + "\\nDatabase: "
                            + tableName.split("\\.")[0]
                            + "\\nTable: "
                            + tableName.split("\\.")[1]
                            + "\"}";
            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = jsonInputString.getBytes("utf-8");
                os.write(input, 0, input.length);
            }
            conn.getResponseCode();
            conn.disconnect();
        } catch (Exception e) {
            LOG.info("Fail to Send Notification to Watchtower {}", watchTowerId);
            LOG.info(e.getMessage());
        }
    }
}
