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

package com.ververica.cdc.connectors.utils;

/** Utilities to get environment variables for testing. */
public class TestEnvUtils {

    private static final String ENV_MODE_KEY = "test.mode";
    private static final String ENV_HOST_KEY = "test.host";
    private static final String ENV_PORT_KEY = "test.port";
    private static final String ENV_USER_KEY = "test.user";
    private static final String ENV_PASSWORD_KEY = "test.password";

    enum TestMode {
        CONTAINER,
        LOCAL;

        public static TestMode parse(String text) {
            if (text != null && text.trim().equalsIgnoreCase("local")) {
                return LOCAL;
            }
            return CONTAINER;
        }
    }

    public static boolean useContainer() {
        return TestMode.parse(System.getenv(ENV_MODE_KEY)).equals(TestMode.CONTAINER);
    }

    public static String getHost() {
        return System.getenv(ENV_HOST_KEY);
    }

    public static int getPort() {
        return Integer.parseInt(System.getenv(ENV_PORT_KEY));
    }

    public static String getUser() {
        return System.getenv(ENV_USER_KEY);
    }

    public static String getPassword() {
        return System.getenv(ENV_PASSWORD_KEY);
    }
}
