-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE full_types (
    id INT AUTO_INCREMENT NOT NULL,
    tiny_c TINYINT,
    tiny_un_c TINYINT UNSIGNED,
    small_c SMALLINT,
    small_un_c SMALLINT UNSIGNED,
    int_c INTEGER ,
    int_un_c INTEGER UNSIGNED,
    int11_c INT(11) ,
    big_c BIGINT,
    varchar_c VARCHAR(255),
    char_c CHAR(3),
    float_c FLOAT,
    double_c DOUBLE,
    decimal_c DECIMAL(8, 4),
    numeric_c NUMERIC(6, 0),
    boolean_c BOOLEAN,
    date_c DATE,
    time_c TIME(0),
    datetime3_c DATETIME(3),
    datetime6_c DATETIME(6),
    timestamp_c TIMESTAMP,
    file_uuid BINARY(16),
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO full_types VALUES (
    DEFAULT, 127, 255, 32767, 65535, 2147483647, 4294967295, 2147483647, 9223372036854775807,
    'Hello World', 'abc', 123.102, 404.4443, 123.4567, 345.6, true,
    '2020-07-17', '18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22',
    unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400','-',''))
);