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

-- Boundary dates around the Julian/Gregorian cutover (1582-10-15) and the
-- Asia/Shanghai LMT switch (1901). Used to verify the snapshot path reads
-- TIMESTAMP / TIMESTAMPTZ / DATE values via java.time types instead of
-- falling through GregorianCalendar.
DROP SCHEMA IF EXISTS historical_dates CASCADE;
CREATE SCHEMA historical_dates;
SET search_path TO historical_dates, public;

CREATE TABLE date_boundary
(
    id   INTEGER NOT NULL,
    ts   TIMESTAMP(6),
    tstz TIMESTAMP(6) WITH TIME ZONE,
    d    DATE,
    PRIMARY KEY (id)
);

ALTER TABLE historical_dates.date_boundary
    REPLICA IDENTITY FULL;

INSERT INTO historical_dates.date_boundary VALUES
    (1, '0001-01-01 00:00:00', '0001-01-01 00:00:00+00', '0001-01-01'),
    (2, '1582-10-04 00:00:00', '1582-10-04 00:00:00+00', '1582-10-04'),
    (3, '1582-10-15 00:00:00', '1582-10-15 00:00:00+00', '1582-10-15'),
    (4, '1900-12-31 23:59:59.123456', '1900-12-31 23:59:59.123456+00', '1900-12-31'),
    (5, '1901-01-02 00:00:00', '1901-01-02 00:00:00+00', '1901-01-02');


CREATE TABLE infinity_boundary (LIKE date_boundary INCLUDING ALL);
ALTER TABLE infinity_boundary REPLICA IDENTITY FULL;
INSERT INTO infinity_boundary VALUES
    (1, 'infinity', 'infinity', 'infinity'),
    (2, '-infinity', '-infinity', '-infinity'),
    (3, NULL, NULL, NULL);

CREATE TABLE bc_boundary (LIKE date_boundary INCLUDING ALL);
ALTER TABLE bc_boundary REPLICA IDENTITY FULL;
INSERT INTO bc_boundary VALUES
    (1, '0001-02-29 12:34:56.123456 BC', '0001-02-29 12:34:56.123456+08 BC', '0001-02-29 BC'),
    (2, '0005-02-29 00:00:00 BC', '0005-02-29 00:00:00+00 BC', '0005-02-29 BC'),
    (3, '0001-02-28 00:00:00 BC', '0001-02-28 00:00:00+00 BC', '0001-02-28 BC'),
    (4, '0001-03-01 00:00:00 BC', '0001-03-01 00:00:00+00 BC', '0001-03-01 BC'),
    (5, '0004-02-29 00:00:00', '0004-02-29 00:00:00+00', '0004-02-29');

CREATE TABLE timestamp_array (id INTEGER PRIMARY KEY, ts TIMESTAMP(6)[], tstz TIMESTAMPTZ[], d DATE[]);
ALTER TABLE timestamp_array REPLICA IDENTITY FULL;
INSERT INTO timestamp_array VALUES
    (1, ARRAY['1900-01-01 00:00:00.123456', '0001-01-01 BC', '0001-02-29 12:34:56.123456 BC', NULL, 'infinity', '-infinity']::TIMESTAMP(6)[],
        ARRAY['0001-02-29 12:34:56.123456+08 BC', NULL, 'infinity', '-infinity']::TIMESTAMPTZ[],
        ARRAY['0001-01-01 BC', '0001-02-29 BC', NULL, 'infinity', '-infinity']::DATE[]),
    (2, '{}', '{}', '{}'),
    (3, NULL, NULL, NULL);
