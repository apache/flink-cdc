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
-- DATABASE:  numeric_zero_precision_test
-- ----------------------------------------------------------------------------------------------------------------
-- Test PostgreSQL numeric(0) fields that previously caused IndexOutOfBoundsException

DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
SET search_path TO inventory, public;

-- Table to test numeric(0) handling - the problematic case that caused IndexOutOfBoundsException
CREATE TABLE numeric_zero_test
(
    id              SERIAL PRIMARY KEY,
    numeric_zero    NUMERIC,        -- This caused IndexOutOfBoundsException
    nullable_zero   NUMERIC,        -- NULL values were especially problematic
    regular_numeric NUMERIC(10, 2),    -- Regular numeric should still work
    big_value       NUMERIC,        -- Large integer stored as numeric(0)
    decimal_value   DECIMAL,        -- Test decimal type as well
    decimal_large   DECIMAL(38, 10),   -- Large decimal value
    bigint_value    BIGINT,         -- Standard bigint for comparison
    bigint_nullable BIGINT,         -- Nullable bigint
    name            VARCHAR(100)
);

ALTER TABLE inventory.numeric_zero_test REPLICA IDENTITY FULL;

-- Insert test data including NULL values that triggered the bug
INSERT INTO inventory.numeric_zero_test (numeric_zero, nullable_zero, regular_numeric, big_value, decimal_value, bigint_value, bigint_nullable, name)
VALUES
    (42, NULL, 123.45, 999999999, 100, 9223372036854775807, NULL, 'test_null'),           -- NULL value case with max bigint
    (null, null, 0.00, null, null, 0, 0, 'test_zeros'),                                   -- Zero values
    (-123, 456, -789.01, 2147483647, -50, -9223372036854775808, 12345678901234, 'test_mixed'),  -- Mixed positive/negative with min bigint
    (NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'test_all_nulls'),                         -- All NULL case
    (999, 888, 77.66, 555444333, 22.11, 1000000000000000000, -1000000000000000000, 'test_bigint_range'); -- Test bigint range

-- Table with numeric(0) arrays - also problematic
CREATE TABLE numeric_zero_array_test
(
    id          SERIAL PRIMARY KEY,
    zero_array  NUMERIC[],   -- Array of numeric(0)
    mixed_data  TEXT
);

ALTER TABLE inventory.numeric_zero_array_test REPLICA IDENTITY FULL;

INSERT INTO inventory.numeric_zero_array_test (zero_array, mixed_data)
VALUES
    ('{1,2,3,NULL}', 'array_with_null'),
    (NULL, 'null_array'),
    ('{}', 'empty_array');
