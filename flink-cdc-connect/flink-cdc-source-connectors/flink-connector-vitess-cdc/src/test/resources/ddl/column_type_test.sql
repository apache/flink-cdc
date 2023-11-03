-- Copyright 2023 Ververica Inc.
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.


USE
test;
DROP TABLE IF EXISTS full_types;
-- TODO add DATE, DATETIME, TIMESTAMP, TIME type mapping
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
--    date_c DATE,
--    time_c TIME(0),
--    datetime3_c DATETIME(3),
--    datetime6_c DATETIME(6),
--    timestamp_c TIMESTAMP,
--    file_uuid BINARY(16),
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;
