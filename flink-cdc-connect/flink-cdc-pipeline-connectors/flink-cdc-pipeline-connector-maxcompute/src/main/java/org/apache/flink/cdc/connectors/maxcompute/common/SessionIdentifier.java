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

package org.apache.flink.cdc.connectors.maxcompute.common;

import org.apache.flink.cdc.common.event.TableId;

import java.io.Serializable;
import java.util.Objects;

/**
 * A Session is uniquely identified through {@link TableId} and {@link String partitionName}. When
 * the Session is successfully created, this class can also carry the sessionId. Note that sessionId
 * does not participate in the comparison of hashcode and equals.
 */
public class SessionIdentifier implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String project;
    private final String schema;
    private final String table;
    private final String partitionName;

    /** sessionId not calculate in hashcode and equals. */
    private String sessionId;

    public SessionIdentifier(String project, String schema, String table, String partitionName) {
        this(project, schema, table, partitionName, null);
    }

    public SessionIdentifier(
            String project, String schema, String table, String partitionName, String sessionId) {
        this.project = project;
        this.schema = schema;
        this.table = table;
        this.partitionName = partitionName;
        this.sessionId = sessionId;
    }

    public static SessionIdentifier of(
            String project, String schema, String table, String partitionName) {
        return new SessionIdentifier(project, schema, table, partitionName);
    }

    public static SessionIdentifier of(
            String project, String schema, String table, String partitionName, String sessionId) {
        return new SessionIdentifier(project, schema, table, partitionName, sessionId);
    }

    public String getProject() {
        return project;
    }

    public String getSchema() {
        return schema;
    }

    public String getTable() {
        return table;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getSessionId() {
        return sessionId;
    }

    @Override
    public String toString() {
        return "SessionIdentifier{"
                + "project='"
                + project
                + '\''
                + ", schema='"
                + schema
                + '\''
                + ", table='"
                + table
                + '\''
                + ", partitionName='"
                + partitionName
                + '\''
                + ", sessionId='"
                + sessionId
                + '\''
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SessionIdentifier that = (SessionIdentifier) o;
        return Objects.equals(project, that.project)
                && Objects.equals(schema, that.schema)
                && Objects.equals(table, that.table)
                && Objects.equals(partitionName, that.partitionName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project, schema, table, partitionName);
    }
}
