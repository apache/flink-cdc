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

package org.apache.flink.cdc.connectors.doris.sink;

import org.apache.doris.flink.catalog.doris.FieldSchema;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.exception.DorisSchemaChangeException;
import org.apache.doris.flink.exception.IllegalArgumentException;
import org.apache.doris.flink.sink.schema.AddColumnPosition;
import org.apache.doris.flink.sink.schema.SchemaChangeManager;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.doris.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.doris.flink.catalog.doris.DorisSchemaFactory.identifier;

/** An enriched version of Doris' {@link SchemaChangeManager}. */
public class DorisSchemaChangeManager extends SchemaChangeManager {
    private static final Logger LOG = LoggerFactory.getLogger(DorisSchemaChangeManager.class);
    private static final int SCHEMA_CHANGE_STATE_RETRY_TIMES = 45;
    private static final long SCHEMA_CHANGE_STATE_RETRY_INTERVAL_MS = 1_000L;
    private static final int HTTP_STATUS_OK = 200;
    private static final String DORIS_SUCCESS_CODE = "0";
    private static final String DORIS_COMMON_ERROR_CODE = "1";
    private static final String DORIS_LEGACY_COMMON_ERROR_CODE = "2";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern HTTP_STATUS =
            Pattern.compile("\\bstatus:\\s*(\\d{3})\\b", Pattern.CASE_INSENSITIVE);
    private static final Pattern TABLE_IN_SCHEMA_CHANGE_STATE =
            Pattern.compile(
                    "table\\[[^]]+]'s state\\(schema_change\\) is not normal\\. "
                            + "do not allow doing alter ops",
                    Pattern.CASE_INSENSITIVE);
    private static final Pattern[] COLUMN_POSITION_ERRORS =
            new Pattern[] {
                Pattern.compile(
                        "can't add value column .+ as first column", Pattern.CASE_INSENSITIVE),
                Pattern.compile(
                        "can't add value column .+ before key column .+", Pattern.CASE_INSENSITIVE),
                Pattern.compile(
                        "can't add key column .+ after value column .+", Pattern.CASE_INSENSITIVE),
                Pattern.compile(
                        "cannot add key column .+ after value column.*", Pattern.CASE_INSENSITIVE),
                Pattern.compile(
                        "can not modify column position after itself", Pattern.CASE_INSENSITIVE),
                Pattern.compile(
                        "can not modify key column .+ after value column",
                        Pattern.CASE_INSENSITIVE),
                Pattern.compile("modify position is invalid", Pattern.CASE_INSENSITIVE),
                // Legacy coarse substrings preserved from the pre-structured classifier.
                // Doris versions differ in wording and some emit these phrases without the
                // "key/value column" prefix the precise patterns above require. Keep matching
                // them so a recoverable column-position error degrades to LAST instead of
                // turning into a hard ADD COLUMN failure. The precise patterns above take
                // precedence; these only widen coverage back to the legacy behavior.
                Pattern.compile("as first column", Pattern.CASE_INSENSITIVE),
                Pattern.compile("before key column", Pattern.CASE_INSENSITIVE),
                Pattern.compile("after value column", Pattern.CASE_INSENSITIVE),
                Pattern.compile("modify column position after itself", Pattern.CASE_INSENSITIVE)
            };

    public DorisSchemaChangeManager(DorisOptions dorisOptions, String charsetEncoding) {
        super(dorisOptions, charsetEncoding);
    }

    @Override
    public boolean execute(String ddl, String databaseName)
            throws IOException, IllegalArgumentException {
        return executeWithBusyRetry(() -> executeOnce(ddl, databaseName), databaseName, ddl);
    }

    protected boolean executeOnce(String ddl, String databaseName)
            throws IOException, IllegalArgumentException {
        return super.execute(ddl, databaseName);
    }

    @Override
    public boolean schemaChange(
            String databaseName, String tableName, Map<String, Object> params, String ddl)
            throws IOException, IllegalArgumentException {
        return executeWithBusyRetry(
                () -> schemaChangeOnce(databaseName, tableName, params, ddl), databaseName, ddl);
    }

    protected boolean schemaChangeOnce(
            String databaseName, String tableName, Map<String, Object> params, String ddl)
            throws IOException, IllegalArgumentException {
        if (checkSchemaChange(databaseName, tableName, params)) {
            return executeOnce(ddl, databaseName);
        }
        return false;
    }

    private boolean executeWithBusyRetry(
            SchemaChangeOperation operation, String databaseName, String ddl)
            throws IOException, IllegalArgumentException {
        int retry = 0;
        while (true) {
            try {
                return operation.execute();
            } catch (DorisSchemaChangeException e) {
                if (!isSchemaChangeBusy(e) || retry >= SCHEMA_CHANGE_STATE_RETRY_TIMES) {
                    throw e;
                }
                retry++;
                LOG.info(
                        "Doris table is still in SCHEMA_CHANGE state while executing DDL. "
                                + "Will retry after {} ms. database={}, retry={}/{}, ddl={}, cause={}",
                        SCHEMA_CHANGE_STATE_RETRY_INTERVAL_MS,
                        databaseName,
                        retry,
                        SCHEMA_CHANGE_STATE_RETRY_TIMES,
                        ddl,
                        e.getMessage());
                sleepBeforeRetry();
            }
        }
    }

    protected void sleepBeforeRetry() {
        try {
            Thread.sleep(SCHEMA_CHANGE_STATE_RETRY_INTERVAL_MS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new DorisSchemaChangeException(
                    "Interrupted while waiting for Doris table schema change to finish.", e);
        }
    }

    @Override
    public boolean addColumn(
            String databaseName,
            String tableName,
            FieldSchema addFieldSchema,
            AddColumnPosition position)
            throws IOException, IllegalArgumentException {
        try {
            return addColumnOnce(databaseName, tableName, addFieldSchema, position);
        } catch (DorisSchemaChangeException e) {
            if (isLastPosition(position) || !isBadColumnPosition(e)) {
                throw e;
            }
            LOG.warn(
                    "Failed to apply ADD COLUMN with Doris column position. "
                            + "Fallback to ADD COLUMN without position. database={}, table={}, column={}, "
                            + "positionType={}, referenceColumn={}, cause={}",
                    databaseName,
                    tableName,
                    addFieldSchema.getName(),
                    position.getPositionType(),
                    position.getReferenceColumn(),
                    e.getMessage());
            return addColumnOnce(databaseName, tableName, addFieldSchema, AddColumnPosition.last());
        }
    }

    protected boolean addColumnOnce(
            String databaseName,
            String tableName,
            FieldSchema addFieldSchema,
            AddColumnPosition position)
            throws IOException, IllegalArgumentException {
        return super.addColumn(databaseName, tableName, addFieldSchema, position);
    }

    public boolean truncateTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String truncateTableDDL =
                "TRUNCATE TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(truncateTableDDL, databaseName);
    }

    public boolean dropTable(String databaseName, String tableName)
            throws IOException, IllegalArgumentException {
        String dropTableDDL =
                "DROP TABLE " + identifier(databaseName) + "." + identifier(tableName);
        return this.execute(dropTableDDL, databaseName);
    }

    public boolean alterTableComment(String databaseName, String tableName, String comment)
            throws IOException, IllegalArgumentException {
        String alterTableCommentDDL =
                "ALTER TABLE "
                        + identifier(databaseName)
                        + "."
                        + identifier(tableName)
                        + " MODIFY COMMENT "
                        + quoted(comment);
        return this.execute(alterTableCommentDDL, databaseName);
    }

    private String quoted(String str) {
        String escaped = str.replace("\\", "\\\\");
        return "\"" + escaped.replace("\"", "\\\"") + "\"";
    }

    private boolean isLastPosition(AddColumnPosition position) {
        return position == null
                || AddColumnPosition.PositionType.LAST.equals(position.getPositionType());
    }

    private boolean isBadColumnPosition(Throwable throwable) {
        return classifyFailure(throwable) == DorisSchemaChangeFailure.COLUMN_POSITION_UNSUPPORTED;
    }

    private boolean isSchemaChangeBusy(Throwable throwable) {
        return classifyFailure(throwable) == DorisSchemaChangeFailure.TABLE_IN_SCHEMA_CHANGE_STATE;
    }

    private DorisSchemaChangeFailure classifyFailure(Throwable throwable) {
        List<String> messages = collectMessages(throwable);
        List<String> unstructuredMessages = new ArrayList<>();
        for (String message : messages) {
            List<DorisErrorResponse> responses = extractErrorResponses(message);
            if (responses.isEmpty()) {
                unstructuredMessages.add(message);
                continue;
            }
            for (DorisErrorResponse response : responses) {
                DorisSchemaChangeFailure failure = classifyResponse(response);
                if (failure != DorisSchemaChangeFailure.OTHER) {
                    return failure;
                }
            }
        }
        return classifyMessage(String.join(" | ", unstructuredMessages));
    }

    private DorisSchemaChangeFailure classifyResponse(DorisErrorResponse response) {
        if (response.isTransportFailure()
                || response.isSuccess()
                || !response.isGenericSqlError()) {
            return DorisSchemaChangeFailure.OTHER;
        }
        // Doris HTTP v2 exposes most SQL failures as generic code=1/2. The code only proves
        // failure, so the business reason still has to come from the structured detail message.
        return classifyMessage(response.getDetailMessage());
    }

    private DorisSchemaChangeFailure classifyMessage(String message) {
        if (message == null || message.trim().isEmpty()) {
            return DorisSchemaChangeFailure.OTHER;
        }
        if (TABLE_IN_SCHEMA_CHANGE_STATE.matcher(message).find()) {
            return DorisSchemaChangeFailure.TABLE_IN_SCHEMA_CHANGE_STATE;
        }
        for (Pattern pattern : COLUMN_POSITION_ERRORS) {
            if (pattern.matcher(message).find()) {
                return DorisSchemaChangeFailure.COLUMN_POSITION_UNSUPPORTED;
            }
        }
        return DorisSchemaChangeFailure.OTHER;
    }

    private List<DorisErrorResponse> extractErrorResponses(String message) {
        List<DorisErrorResponse> responses = new ArrayList<>();
        if (message == null) {
            return responses;
        }
        Optional<Integer> httpStatus = extractHttpStatus(message);
        for (String json : extractJsonObjects(message)) {
            try {
                JsonNode response = OBJECT_MAPPER.readTree(json);
                responses.add(
                        new DorisErrorResponse(
                                httpStatus.orElse(null),
                                text(response.path("code")),
                                detailMessage(response)));
            } catch (Exception ignored) {
                // Not a Doris schema-change response JSON. Try the next embedded object.
            }
        }
        if (responses.isEmpty() && httpStatus.isPresent()) {
            responses.add(new DorisErrorResponse(httpStatus.get(), null, message));
        }
        return responses;
    }

    private Optional<Integer> extractHttpStatus(String message) {
        Matcher matcher = HTTP_STATUS.matcher(message);
        if (!matcher.find()) {
            return Optional.empty();
        }
        try {
            return Optional.of(Integer.parseInt(matcher.group(1)));
        } catch (NumberFormatException ignored) {
            return Optional.empty();
        }
    }

    private String detailMessage(JsonNode response) {
        String detailMessage = text(response.path("detailMessage"));
        if (detailMessage != null) {
            return detailMessage;
        }

        JsonNode data = response.path("data");
        if (data.isObject()) {
            detailMessage = text(data.path("detailMessage"));
            if (detailMessage != null) {
                return detailMessage;
            }
        }
        if (!data.isMissingNode() && !data.isNull()) {
            return data.isTextual() ? data.asText() : data.toString();
        }
        return text(response.path("msg"));
    }

    private String text(JsonNode node) {
        if (node == null || node.isMissingNode() || node.isNull()) {
            return null;
        }
        String value = node.asText();
        return value == null || value.trim().isEmpty() ? null : value;
    }

    private List<String> extractJsonObjects(String message) {
        List<String> jsonObjects = new ArrayList<>();
        for (int i = 0; i < message.length(); i++) {
            if (message.charAt(i) != '{') {
                continue;
            }
            int end = findJsonObjectEnd(message, i);
            if (end >= 0) {
                jsonObjects.add(message.substring(i, end + 1));
                i = end;
            }
        }
        return jsonObjects;
    }

    private int findJsonObjectEnd(String text, int start) {
        int depth = 0;
        boolean inString = false;
        boolean escaped = false;
        for (int i = start; i < text.length(); i++) {
            char c = text.charAt(i);
            if (escaped) {
                escaped = false;
                continue;
            }
            if (c == '\\') {
                escaped = inString;
                continue;
            }
            if (c == '"') {
                inString = !inString;
                continue;
            }
            if (inString) {
                continue;
            }
            if (c == '{') {
                depth++;
            } else if (c == '}') {
                depth--;
                if (depth == 0) {
                    return i;
                }
            }
        }
        return -1;
    }

    private List<String> collectMessages(Throwable throwable) {
        List<String> messages = new ArrayList<>();
        Throwable current = throwable;
        while (current != null) {
            if (current.getMessage() != null) {
                messages.add(current.getMessage());
            }
            current = current.getCause();
        }
        return messages;
    }

    @FunctionalInterface
    private interface SchemaChangeOperation {
        boolean execute() throws IOException, IllegalArgumentException;
    }

    private enum DorisSchemaChangeFailure {
        TABLE_IN_SCHEMA_CHANGE_STATE,
        COLUMN_POSITION_UNSUPPORTED,
        OTHER
    }

    private static class DorisErrorResponse {
        private final Integer httpStatus;
        private final String code;
        private final String detailMessage;

        private DorisErrorResponse(Integer httpStatus, String code, String detailMessage) {
            this.httpStatus = httpStatus;
            this.code = code;
            this.detailMessage = detailMessage;
        }

        private boolean isTransportFailure() {
            return httpStatus != null && httpStatus != HTTP_STATUS_OK;
        }

        private boolean isSuccess() {
            return DORIS_SUCCESS_CODE.equals(code);
        }

        private boolean isGenericSqlError() {
            return code == null
                    || DORIS_COMMON_ERROR_CODE.equals(code)
                    || DORIS_LEGACY_COMMON_ERROR_CODE.equals(code);
        }

        private String getDetailMessage() {
            return detailMessage;
        }
    }
}
