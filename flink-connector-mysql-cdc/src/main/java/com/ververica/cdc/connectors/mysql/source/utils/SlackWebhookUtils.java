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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/** Send notification to slack. */
public class SlackWebhookUtils {
    private static final Logger LOG = LoggerFactory.getLogger(SlackWebhookUtils.class);

    private static String removeNewlines(String text) {
        return text.replace("\n", "").replace("\r", "");
    }

    private static void sendPayload(HttpURLConnection conn, String payload) throws IOException {
        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = payload.getBytes(StandardCharsets.UTF_8);
            os.write(input, 0, input.length);
        }
        conn.getResponseCode();
        conn.disconnect();
    }

    private static HttpURLConnection createConnection(String hookUrl) throws IOException {
        URL url = new URL(hookUrl);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setDoInput(true);
        conn.setConnectTimeout(5000);
        conn.setRequestProperty("Content-Type", "application/json");

        return conn;
    }

    public static void notify(String hookUrl, String header, String tableName, String gtids) {

        try {
            HttpURLConnection conn = createConnection(hookUrl);

            String database = removeNewlines(tableName.split("\\.")[0]);
            String table = removeNewlines(tableName.split("\\.")[1]);
            String gtidsInfo =
                    !gtids.isEmpty() ? String.format("\\nGTIDs: %s", removeNewlines(gtids)) : "";
            String payload =
                    String.format(
                            "{\"text\":\"[%s]\\nDatabase: %s\\nTable: %s%s\"}",
                            header, database, table, gtidsInfo);
            sendPayload(conn, payload);
        } catch (Exception e) {
            LOG.info("Fail to Send Notification to Slack");
            LOG.info(e.getMessage());
        }
    }
}
