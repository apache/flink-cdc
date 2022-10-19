-- Copyright 2022 Ververica Inc.
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

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE DATABASE column_type_test;
USE column_type_test;

CREATE TABLE full_types
(
    id            INT AUTO_INCREMENT NOT NULL,
    bit1_c        BIT,
    tiny1_c       TINYINT(1),
    boolean_c     BOOLEAN,
    tiny_c        TINYINT,
    tiny_un_c     TINYINT UNSIGNED,
    small_c       SMALLINT,
    small_un_c    SMALLINT UNSIGNED,
    medium_c      MEDIUMINT,
    medium_un_c   MEDIUMINT UNSIGNED,
    int11_c       INT(11),
    int_c         INTEGER,
    int_un_c      INTEGER UNSIGNED,
    big_c         BIGINT,
    big_un_c      BIGINT UNSIGNED,
    real_c        REAL,
    float_c       FLOAT,
    double_c      DOUBLE,
    decimal_c     DECIMAL(8, 4),
    numeric_c     NUMERIC(6, 0),
    big_decimal_c DECIMAL(65, 1),
    date_c        DATE,
    time_c        TIME(0),
    datetime3_c   DATETIME(3),
    datetime6_c   DATETIME(6),
    timestamp_c   TIMESTAMP,
    timestamp3_c  TIMESTAMP(3),
    timestamp6_c  TIMESTAMP(6),
    char_c        CHAR(3),
    varchar_c     VARCHAR(255),
    file_uuid     BINARY(16),
    bit_c         BIT(64),
    text_c        TEXT,
    tiny_blob_c   TINYBLOB,
    medium_blob_c MEDIUMBLOB,
    blob_c        BLOB,
    long_blob_c   LONGBLOB,
    year_c        YEAR,
    set_c         SET ('a', 'b'),
    enum_c        ENUM ('red', 'green', 'blue'),
    json_c        JSON,
    PRIMARY KEY (id)
) DEFAULT CHARSET = utf8mb4;

INSERT INTO full_types
VALUES (DEFAULT, 0, 1, true, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 2147483647, 4294967295,
        9223372036854775807, 18446744073709551615, 123.102, 123.102, 404.4443, 123.4567, 345.6, 34567892.1,
        '2020-07-17', '18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17 18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        'abc', 'Hello World', unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400', '-', '')),
        b'0000010000000100000001000000010000000100000001000000010000000100', 'text', UNHEX(HEX(16)), UNHEX(HEX(16)),
        UNHEX(HEX(16)), UNHEX(HEX(16)), 2022, 'a,b,a', 'red', '{"key1":"value1"}');
