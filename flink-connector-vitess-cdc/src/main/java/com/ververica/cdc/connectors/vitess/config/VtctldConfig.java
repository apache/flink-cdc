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

package com.ververica.cdc.connectors.vitess.config;

import java.util.Objects;

/** VTCtld server configuration options. */
public class VtctldConfig {

    private String hostname;
    private int port = 15999; // default 15999 port
    private String username;
    private String password;

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static VtctldConfig.Builder builder() {
        return new VtctldConfig.Builder();
    }

    /** Builder class of {@link VtctldConfig}. */
    public static final class Builder {
        private String hostname;
        private int port = 15999; // default 15999 port
        private String username;
        private String password;

        /** IP address or hostname of the VTCtld server. */
        public Builder hostname(String hostname) {
            this.hostname = hostname;
            return this;
        }

        /** Integer port number of the VTCtld server. */
        public Builder port(int port) {
            this.port = port;
            return this;
        }

        /**
         * An optional username of the VTCtld server. If not configured, unauthenticated VTCtld gRPC
         * is used.
         */
        public Builder username(String username) {
            this.username = username;
            return this;
        }

        /**
         * An optional password of the VTCtld server. If not configured, unauthenticated VTCtld gRPC
         * is used.
         */
        public Builder password(String password) {
            this.password = password;
            return this;
        }

        public VtctldConfig build() {
            VtctldConfig vtctldConfig = new VtctldConfig();
            vtctldConfig.password = this.password;
            vtctldConfig.username = this.username;
            vtctldConfig.hostname = this.hostname;
            vtctldConfig.port = this.port;
            return vtctldConfig;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VtctldConfig that = (VtctldConfig) o;
        return port == that.port
                && Objects.equals(hostname, that.hostname)
                && Objects.equals(username, that.username)
                && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(hostname, port, username, password);
    }

    @Override
    public String toString() {
        return "VtctldConfig{"
                + "hostname='"
                + hostname
                + '\''
                + ", port="
                + port
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + '}';
    }
}
