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

CREATE TABLE common_types
(
    id                   SERIAL,
    tiny_c               TINYINT,
    tiny_un_c            TINYINT UNSIGNED,
    tiny_un_z_c          TINYINT UNSIGNED ZEROFILL,
    small_c              SMALLINT,
    small_un_c           SMALLINT UNSIGNED,
    small_un_z_c         SMALLINT UNSIGNED ZEROFILL,
    medium_c             MEDIUMINT,
    medium_un_c          MEDIUMINT UNSIGNED,
    medium_un_z_c        MEDIUMINT UNSIGNED ZEROFILL,
    int_c                INTEGER,
    int_un_c             INTEGER UNSIGNED,
    int_un_z_c           INTEGER UNSIGNED ZEROFILL,
    int11_c              INT(11),
    big_c                BIGINT,
    big_un_c             BIGINT UNSIGNED,
    big_un_z_c           BIGINT UNSIGNED ZEROFILL,
    varchar_c            VARCHAR(255),
    char_c               CHAR(3),
    real_c               REAL,
    float_c              FLOAT,
    float_un_c           FLOAT UNSIGNED,
    float_un_z_c         FLOAT UNSIGNED ZEROFILL,
    double_c             DOUBLE,
    double_un_c          DOUBLE UNSIGNED,
    double_un_z_c        DOUBLE UNSIGNED ZEROFILL,
    decimal_c            DECIMAL(8, 4),
    decimal_un_c         DECIMAL(8, 4) UNSIGNED,
    decimal_un_z_c       DECIMAL(8, 4) UNSIGNED ZEROFILL,
    numeric_c            NUMERIC(6, 0),
    big_decimal_c        DECIMAL(65, 1),
    bit1_c               BIT,
    bit3_c               BIT(3),
    tiny1_c              TINYINT(1),
    boolean_c            BOOLEAN,
    file_uuid            BINARY(16),
    bit_c                BIT(64),
    text_c               TEXT,
    tiny_blob_c          TINYBLOB,
    blob_c               BLOB,
    medium_blob_c        MEDIUMBLOB,
    long_blob_c          LONGBLOB,
    year_c YEAR,
    enum_c               enum('red', 'white') default 'red',
    json_c               JSON,
    point_c              POINT,
    geometry_c           GEOMETRY,
    linestring_c         LINESTRING,
    polygon_c            POLYGON,
    multipoint_c         MULTIPOINT,
    multiline_c          MULTILINESTRING,
    multipolygon_c       MULTIPOLYGON,
    geometrycollection_c GEOMETRYCOLLECTION,
    long_c               LONG,
    long_varchar_c       LONG VARCHAR,
    varchar_len0_c        VARCHAR(0),
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO common_types
VALUES (DEFAULT, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647,
        4294967295, 4294967295, 2147483647, 9223372036854775807,
        18446744073709551615, 18446744073709551615,
        'Hello World', 'abc', 123.102, 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445,
        123.4567, 123.4568, 123.4569, 345.6, 34567892.1,
        0, b'011', 1, true,
        unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400', '-', '')),
        b'0000010000000100000001000000010000000100000001000000010000000100',
        'text', UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), 2021,
        'red',
        '{"key1":"value1"}',
        ST_GeomFromText('POINT(1 1)'),
        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
        ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),
        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
        ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
        ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
        ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),
        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'),
        'long','long varchar',
        '');

CREATE TABLE time_types
(
    id                   SERIAL,
    year_c               YEAR,
    date_c               DATE,
    time_c               TIME(0),
    time_3_c             TIME(3),
    time_6_c             TIME(6),
    datetime_c           DATETIME(0),
    datetime3_c          DATETIME(3),
    datetime6_c          DATETIME(6),
    timestamp_c          TIMESTAMP(0),
    timestamp3_c         TIMESTAMP(3),
    timestamp6_c         TIMESTAMP(6),
    timestamp_def_c      TIMESTAMP NULL DEFAULT '2000-01-01 00:00:00',
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO time_types
VALUES (DEFAULT,
        '2021',
        '2020-07-17',
        '18:00:22',
        '18:00:22.123',
        '18:00:22.123456',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22.123',
        '2020-07-17 18:00:22.123456',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22.123',
        '2020-07-17 18:00:22.123456',
        NULL);

CREATE TABLE precision_types
(
    id                   SERIAL,
    decimal_c0           DECIMAL(6, 2),
    decimal_c1           DECIMAL(9, 4),
    decimal_c2           DECIMAL(20, 4),
    time_c               TIME(0),
    time_3_c             TIME(3),
    time_6_c             TIME(6),
    datetime_c           DATETIME(0),
    datetime3_c          DATETIME(3),
    datetime6_c          DATETIME(6),
    timestamp_c          TIMESTAMP(0) NULL,
    timestamp3_c         TIMESTAMP(3) NULL,
    timestamp6_c         TIMESTAMP(6) NULL,
    float_c0             FLOAT(6, 0),
    float_c1             FLOAT(20, 3),
    float_c2             FLOAT(24, 12),
    real_c0              REAL(6, 0),
    real_c1              REAL(20, 3),
    real_c2              REAL(24, 12),
    double_c0            DOUBLE(6, 0),
    double_c1            DOUBLE(20, 3),
    double_c2            DOUBLE(24, 12),
    double_precision_c0  DOUBLE PRECISION(6, 0),
    double_precision_c1  DOUBLE PRECISION(20, 3),
    double_precision_c2  DOUBLE PRECISION(24, 12),
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO precision_types
VALUES (DEFAULT,
        123.4,
        1234.5,
        1234.56,
        '18:00',
        '18:00:22.1',
        '18:00:22.1',
        '2020-07-17 18:00',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22',
        2,
        3,
        5,
        7,
        11,
        13,
        17,
        19,
        23,
        29,
        31,
        37);

CREATE TABLE json_types
(
    id                   SERIAL,
    json_c0               JSON,
    json_c1               JSON,
    json_c2               JSON,
    int_c  INTEGER,
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO json_types
VALUES (DEFAULT,
        '{"key1":"value1"}',
        '{"key1":"value1","key2":"value2"}',
        '[{"key1":"value1","key2":{"key2_1":"value2_1","key2_2":"value2_2"},"key3":["value3"],"key4":["value4_1","value4_2"]},{"key5":"value5"}]',
        1
        );