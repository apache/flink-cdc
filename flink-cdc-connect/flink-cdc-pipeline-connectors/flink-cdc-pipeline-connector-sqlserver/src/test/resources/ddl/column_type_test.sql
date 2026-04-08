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
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------
-- Create the column_type_test database
CREATE DATABASE column_type_test;

USE column_type_test;

-- Avoid SqlServer error com.microsoft.sqlserver.jdbc.SQLServerException: Could not update the metadata that indicates,
-- the root cause is 14258: 'Cannot perform this operation while SQLServerAgent is starting. Try again later.'. We simply
-- wait for 3 seconds to improve the test stabilises.
WAITFOR DELAY '00:00:03';
EXEC sys.sp_cdc_enable_db;

-- Create full_types table for testing all supported data types
CREATE TABLE full_types (
    id int NOT NULL PRIMARY KEY,
    -- Character types
    val_char char(3),
    val_varchar varchar(1000),
    val_text text,
    val_nchar nchar(3),
    val_nvarchar nvarchar(1000),
    val_ntext ntext,
    -- Numeric types
    val_decimal decimal(6,3),
    val_numeric numeric(10,2),
    val_float float,
    val_real real,
    val_smallmoney smallmoney,
    val_money money,
    -- Boolean and integer types
    val_bit bit,
    val_tinyint tinyint,
    val_smallint smallint,
    val_int int,
    val_bigint bigint,
    -- Date and time types
    val_date date,
    val_time_p2 time(2),
    val_time time(4),
    val_datetime2 datetime2,
    val_datetimeoffset datetimeoffset,
    val_datetime datetime,
    val_smalldatetime smalldatetime,
    -- Other types
    val_xml xml,
    val_binary binary(8),
    val_varbinary varbinary(100),
    val_uniqueidentifier uniqueidentifier
);

INSERT INTO full_types VALUES (
    1,
    -- Character types
    'abc', 'varchar value', 'text value', N'中文', N'nvarchar value', N'ntext value',
    -- Numeric types
    123.456, 9876543.21, 3.14159265358979, 2.71828, 214748.3647, 5477.5807,
    -- Boolean and integer types
    1, 255, 32767, 2147483647, 9223372036854775807,
    -- Date and time types
    '2020-07-17', '18:00:22.12', '18:00:22.1234', '2020-07-17 18:00:22.123456',
    '2020-07-17 18:00:22.1234567 +00:00', '2020-07-17 18:00:22.123', '2020-07-17 18:00:00',
    -- Other types
    '<root><child>value</child></root>', 0x0102030405060708, 0x48656C6C6F, 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'
);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'full_types', @role_name = NULL, @supports_net_changes = 0;

-- Create time_types table for testing time-related types
CREATE TABLE time_types (
    id int NOT NULL PRIMARY KEY,
    val_date date,
    val_time_0 time(0),
    val_time_3 time(3),
    val_time_6 time(6),
    val_datetime2_0 datetime2(0),
    val_datetime2_3 datetime2(3),
    val_datetime2_6 datetime2(6),
    val_datetimeoffset_0 datetimeoffset(0),
    val_datetimeoffset_3 datetimeoffset(3),
    val_datetimeoffset_6 datetimeoffset(6),
    val_datetime datetime,
    val_smalldatetime smalldatetime
);

INSERT INTO time_types VALUES (
    1,
    '2020-07-17',
    '18:00:22', '18:00:22.123', '18:00:22.123456',
    '2020-07-17 18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
    '2020-07-17 18:00:22 +00:00', '2020-07-17 18:00:22.123 +00:00', '2020-07-17 18:00:22.123456 +00:00',
    '2020-07-17 18:00:22.123', '2020-07-17 18:00:00'
);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'time_types', @role_name = NULL, @supports_net_changes = 0;

-- Create precision_types table for testing decimal precision
CREATE TABLE precision_types (
    id int NOT NULL PRIMARY KEY,
    val_decimal_6_2 decimal(6,2),
    val_decimal_10_4 decimal(10,4),
    val_decimal_20_6 decimal(20,6),
    val_decimal_38_10 decimal(38,10),
    val_numeric_6_2 numeric(6,2),
    val_numeric_10_4 numeric(10,4),
    val_float float,
    val_real real,
    val_money money,
    val_smallmoney smallmoney
);

INSERT INTO precision_types VALUES (
    1,
    1234.56, 123456.7890, 12345678901234.567890, 1234567890123456789012345678.9012345678,
    1234.56, 123456.7890,
    3.141592653589793, 2.7182818,
    922337203685477.5807, 214748.3647
);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'precision_types', @role_name = NULL, @supports_net_changes = 0;
