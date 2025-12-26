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

DROP SCHEMA IF EXISTS replication_test CASCADE;
CREATE SCHEMA replication_test;

-- FULL 复制标识（所有列）
CREATE TABLE replication_test.full_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);
ALTER TABLE replication_test.full_replica REPLICA IDENTITY FULL;

-- DEFAULT 复制标识（主键）
CREATE TABLE replication_test.default_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);

-- NOTHING 复制标识（无复制）
CREATE TABLE replication_test.nothing_replica (
    id INT PRIMARY KEY,
    name VARCHAR(50),
    value INT
);
ALTER TABLE replication_test.nothing_replica REPLICA IDENTITY NOTHING;

INSERT INTO replication_test.full_replica VALUES (1, 'test1', 100);
INSERT INTO replication_test.default_replica VALUES (2, 'test2', 200);
INSERT INTO replication_test.nothing_replica VALUES (3, 'test3', 300);

