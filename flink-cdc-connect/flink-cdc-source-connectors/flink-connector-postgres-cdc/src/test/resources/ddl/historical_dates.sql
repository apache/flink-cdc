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
