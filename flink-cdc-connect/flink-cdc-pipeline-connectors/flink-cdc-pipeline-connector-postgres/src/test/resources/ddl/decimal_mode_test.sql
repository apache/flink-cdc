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
-- DATABASE:  decimal_mode_test
-- ----------------------------------------------------------------------------------------------------------------
-- Generate a number of tables to cover as many of the PG types as possible


DROP SCHEMA IF EXISTS test_decimal CASCADE;
CREATE SCHEMA IF NOT EXISTS test_decimal;

SET search_path TO test_decimal;

DROP TABLE IF EXISTS decimal_test_table;
CREATE TABLE decimal_test_table (
                                    id SERIAL PRIMARY KEY,
                                    fixed_numeric NUMERIC(10,2),
                                    fixed_decimal DECIMAL(8,4),
                                    variable_numeric NUMERIC,
                                    variable_decimal DECIMAL,
                                    amount_money MONEY
);

ALTER TABLE decimal_test_table REPLICA IDENTITY FULL;

INSERT INTO decimal_test_table (
    id,
    fixed_numeric,
    fixed_decimal,
    variable_numeric,
    variable_decimal,
    amount_money
) VALUES
(1, 123.45, 67.8912, 987.65, 12.3, '100.50'::money);


DROP TABLE IF EXISTS decimal_test_zero;
CREATE TABLE decimal_test_zero (
                                    id SERIAL PRIMARY KEY,
                                    fixed_numeric NUMERIC(10,2),
                                    fixed_decimal DECIMAL(8,4),
                                    variable_numeric NUMERIC,
                                    variable_decimal DECIMAL,
                                    amount_money MONEY
);

ALTER TABLE decimal_test_zero REPLICA IDENTITY FULL;

INSERT INTO decimal_test_zero (
    id,
    fixed_numeric,
    fixed_decimal,
    variable_numeric,
    variable_decimal,
    amount_money
) VALUES
    (2, 99999999.99, 9999.9999, null, null, null);