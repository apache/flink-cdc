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

CREATE TABLE full_types
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
    tiny1_c              TINYINT(1),
    boolean_c            BOOLEAN,
    date_c               DATE,
    time_c               TIME(0),
    datetime3_c          DATETIME(3),
    datetime6_c          DATETIME(6),
    timestamp_c          TIMESTAMP,
    file_uuid            BINARY(16),
    bit_c                BIT(64),
    text_c               TEXT,
    tiny_blob_c          TINYBLOB,
    blob_c               BLOB,
    medium_blob_c        MEDIUMBLOB,
    long_blob_c          LONGBLOB,
    year_c YEAR,
    enum_c               enum('red', 'white') default 'red',
    set_c SET('a', 'b'),
    json_c               JSON,
    point_c              POINT,
    geometry_c           GEOMETRY,
    linestring_c         LINESTRING,
    polygon_c            POLYGON,
    multipoint_c         MULTIPOINT,
    multiline_c          MULTILINESTRING,
    multipolygon_c       MULTIPOLYGON,
    geometrycollection_c GEOMCOLLECTION,
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO full_types
VALUES (DEFAULT, 127, 255, 255, 32767, 65535, 65535, 8388607, 16777215, 16777215, 2147483647,
        4294967295, 4294967295, 2147483647, 9223372036854775807,
        18446744073709551615, 18446744073709551615,
        'Hello World', 'abc', 123.102, 123.102, 123.103, 123.104, 404.4443, 404.4444, 404.4445,
        123.4567, 123.4568, 123.4569, 345.6, 34567892.1, 0, 1, true,
        '2020-07-17', '18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22',
        unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400', '-', '')),
        b'0000010000000100000001000000010000000100000001000000010000000100',
        'text', UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), UNHEX(HEX(16)), 2021,
        'red', 'a,b,a', '{
    "key1": "value1"
  }',
        ST_GeomFromText('POINT(1 1)'),
        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
        ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),
        ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
        ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
        ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
        ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),
        ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))'));