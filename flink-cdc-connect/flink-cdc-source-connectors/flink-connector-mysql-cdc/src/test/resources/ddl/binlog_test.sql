-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  binlog_test
-- ----------------------------------------------------------------------------------------------------------------
-- This database is used for testing binlog-only newly added table capture functionality.
--
-- The test validates that:
-- 1. Tables created dynamically during binlog reading phase are automatically captured
-- 2. Data changes in newly added tables are captured as binlog events (not snapshots)
-- 3. Table pattern matching works correctly for newly added tables
-- 4. Non-matching tables are not captured
--
-- IMPORTANT: This SQL file defines the initial schema for reference and documentation.
-- The actual test creates tables dynamically during execution to validate binlog-only capture.
-- The initial_table is crea1ted in @BeforeEach to ensure binlog is active before CDC source starts.

-- Initial table to activate binlog
-- This table is actually created in test code, but defined here for reference
CREATE TABLE initial_table (
    id BIGINT PRIMARY KEY,
    value VARCHAR(100)
);

INSERT INTO initial_table VALUES (1, 'initial');