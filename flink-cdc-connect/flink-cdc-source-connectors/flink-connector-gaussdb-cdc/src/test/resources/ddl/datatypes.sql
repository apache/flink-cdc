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

DROP SCHEMA IF EXISTS test_types CASCADE;
CREATE SCHEMA test_types;

-- 数值类型
CREATE TABLE test_types.numeric_types (
    id INT PRIMARY KEY,
    smallint_col SMALLINT,
    integer_col INTEGER,
    bigint_col BIGINT,
    decimal_col DECIMAL(10, 2),
    numeric_col NUMERIC(15, 5),
    real_col REAL,
    double_col DOUBLE PRECISION
);

-- 字符类型
CREATE TABLE test_types.string_types (
    id INT PRIMARY KEY,
    char_col CHAR(10),
    varchar_col VARCHAR(100),
    text_col TEXT
);

-- 时间类型
CREATE TABLE test_types.temporal_types (
    id INT PRIMARY KEY,
    date_col DATE,
    time_col TIME,
    timestamp_col TIMESTAMP,
    timestamptz_col TIMESTAMP WITH TIME ZONE
);

-- 布尔和特殊类型
CREATE TABLE test_types.special_types (
    id INT PRIMARY KEY,
    boolean_col BOOLEAN,
    uuid_col UUID,
    json_col JSON,
    array_col INTEGER[]
);

-- 插入测试数据
INSERT INTO test_types.numeric_types VALUES
    (1, 100, 10000, 1000000000, 123.45, 12345.67890, 3.14, 2.718281828);

INSERT INTO test_types.string_types VALUES
    (1, 'test', 'variable length', 'This is a long text field for testing');

INSERT INTO test_types.temporal_types VALUES
    (1, '2024-01-15', '14:30:00', '2024-01-15 14:30:00', '2024-01-15 14:30:00+08');

INSERT INTO test_types.special_types VALUES
    (1, true, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', '{"key": "value"}', ARRAY[1,2,3,4,5]);

