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
-- Generate a number of tables to cover as many of the PG types as possible
DROP SCHEMA IF EXISTS inventory CASCADE;
CREATE SCHEMA inventory;
-- postgis is installed into public schema
SET search_path TO inventory, public;
CREATE EXTENSION IF NOT EXISTS ltree;
CREATE EXTENSION IF NOT EXISTS citext;
CREATE EXTENSION IF NOT EXISTS hstore;

CREATE TYPE status AS ENUM ('pending', 'approved', 'rejected');

CREATE TABLE full_types
(
    id                  INTEGER NOT NULL,
    bytea_c             BYTEA,
    small_c             SMALLINT,
    int_c               INTEGER,
    big_c               BIGINT,
    real_c              REAL,
    double_precision    DOUBLE PRECISION,
    numeric_c           NUMERIC(10, 5),
    decimal_c           DECIMAL(10, 1),
    boolean_c           BOOLEAN,
    text_c              TEXT,
    char_c              CHAR,
    character_c         CHARACTER(3),
    character_varying_c CHARACTER VARYING(20),
    timestamp3_c        TIMESTAMP(3),
    timestamp6_c        TIMESTAMP(6),
    date_c              DATE,
    time_c              TIME(0),
    default_numeric_c   NUMERIC,
    geometry_c          GEOMETRY(POINT, 3187),
    geography_c         GEOGRAPHY(MULTILINESTRING),
    bit_c               BIT(1),
    bit_fixed_c         BIT(8),
    bit_varying_c       BIT VARYING(20),
    bpchar_c            BPCHAR(3),
    duration_c          INTERVAL,
    json_c              JSON,
    jsonb_c             JSONB,
    xml_C               XML,
    location            POINT,
    ltree_c               LTREE,
    username CITEXT NOT NULL,
    attributes HSTORE,
    inet_c              INET,
    int4range_c           INT4RANGE,
    int8range_c            INT8RANGE,
    numrange_c            NUMRANGE,
    tsrange_c TSRANGE,
    daterange_c DATERANGE,
    status status NOT NULL,
    PRIMARY KEY (id)
);

ALTER TABLE inventory.full_types
    REPLICA IDENTITY FULL;

INSERT INTO inventory.full_types
VALUES (1, '2', 32767, 65535, 2147483647, 5.5, 6.6, 123.12345, 404.4443, true,
        'Hello World', 'a', 'abc', 'abcd..xyz', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456',
        '2020-07-17', '18:00:22', 500,'SRID=3187;POINT(174.9479 -36.7208)'::geometry,
        'MULTILINESTRING((169.1321 -44.7032, 167.8974 -44.6414))'::geography,B'1',B'00001010',B'00101010','abc','2 weeks','{"order_id": 10248, "product": "Notebook", "quantity": 5}','{"order_id": 10249, "product": "Pen", "quantity": 10}'::jsonb,'<user>
        <id>123</id>
        <name>Alice</name>
        <email>alice@example.com</email>
        <preferences>
            <theme>dark</theme>
            <notifications>true</notifications>
        </preferences>
    </user>','(3.456,7.890)'::point,'foo.bar.baz','JohnDoe','color => "blue", size => "L"','192.168.1.1'::inet,'[1, 10)'::int4range,'[1000000000, 5000000000)'::int8range,'[5.5, 20.75)'::numrange,
       '["2023-08-01 08:00:00", "2023-08-01 12:00:00")','["2023-08-01", "2023-08-15")','pending');


CREATE TABLE time_types (
                            id                   SERIAL PRIMARY KEY,
                            date_c               DATE,
                            time_c               TIME(0) WITHOUT TIME ZONE,
                            time_3_c             TIME(3) WITHOUT TIME ZONE,
                            time_6_c             TIME(6) WITHOUT TIME ZONE,
                            datetime_c           TIMESTAMP(0) WITHOUT TIME ZONE,
                            datetime3_c          TIMESTAMP(3) WITHOUT TIME ZONE,
                            datetime6_c          TIMESTAMP(6) WITHOUT TIME ZONE,
                            timestamp_c          TIMESTAMP WITHOUT TIME ZONE,
                            timestamp_tz_c       TIMESTAMP WITH TIME ZONE
);
ALTER TABLE inventory.time_types
    REPLICA IDENTITY FULL;

INSERT INTO time_types
VALUES (2,
        '2020-07-17',
        '18:00:22',
        '18:00:22.123',
        '18:00:22.123456',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22.123',
        '2020-07-17 18:00:22.123456',
        '2020-07-17 18:00:22',
        '2020-07-17 18:00:22+08:00');

CREATE TABLE hstore_types (
  id SERIAL PRIMARY KEY,
  hstore_c HSTORE
);

ALTER TABLE inventory.hstore_types
    REPLICA IDENTITY FULL;

INSERT INTO hstore_types
VALUES (1, 'a => 1, b => 2');

CREATE TABLE json_types (
                            id        SERIAL PRIMARY KEY,
                            json_c0   JSON,
                            json_c1   JSON,
                            json_c2   JSON,
                            jsonb_c0   JSONB,
                            jsonb_c1   JSONB,
                            jsonb_c2   JSONB,
                            int_c     INTEGER
);

ALTER TABLE inventory.json_types
    REPLICA IDENTITY FULL;

INSERT INTO json_types (id,json_c0, json_c1, json_c2, jsonb_c0, jsonb_c1, jsonb_c2, int_c)
VALUES
    (1,
        '{"key1":"value1"}',
        '{"key1":"value1","key2":"value2"}',
        '[{"key1":"value1","key2":{"key2_1":"value2_1","key2_2":"value2_2"},"key3":["value3"],"key4":["value4_1","value4_2"]},{"key5":"value5"}]',
        '{"key1":"value1"}'::jsonb,
        '{"key1":"value1","key2":"value2"}'::jsonb,
        '[{"key1":"value1","key2":{"key2_1":"value2_1","key2_2":"value2_2"},"key3":["value3"],"key4":["value4_1","value4_2"]},{"key5":"value5"}]'::jsonb,
     1
    );

CREATE TABLE array_types (
                            id        SERIAL PRIMARY KEY,
                            text_a1  TEXT[],
                            int_a1    INTEGER[],
                            int_s1 INTEGER[]
);

ALTER TABLE inventory.array_types
    REPLICA IDENTITY FULL;

INSERT INTO array_types (id,text_a1, int_a1, int_s1)
VALUES
    (1,
     ARRAY['electronics', 'gadget', 'sale'],
     '{85, 90, 78}',
     '{42}'
    );

CREATE TABLE array_types_unsupported_matrix (
                             id        SERIAL PRIMARY KEY,
                             matrix_a1 INTEGER[][]
);

INSERT INTO array_types_unsupported_matrix (id,matrix_a1)
VALUES
    (1,
     '{{1,2},{3,4}}'
    );