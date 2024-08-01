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
-- DATABASE:  mysql_inventory
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our products using a single insert with many rows
CREATE TABLE timestamps (
    id INTEGER NOT NULL PRIMARY KEY,
    ts0 TIMESTAMP(0),
    ts1 TIMESTAMP(1),
    ts2 TIMESTAMP(2),
    ts3 TIMESTAMP(3),
    ts4 TIMESTAMP(4),
    ts5 TIMESTAMP(5),
    ts6 TIMESTAMP(6),
    dt0 DATETIME(0),
    dt1 DATETIME(1),
    dt2 DATETIME(2),
    dt3 DATETIME(3),
    dt4 DATETIME(4),
    dt5 DATETIME(5),
    dt6 DATETIME(6)
);

INSERT INTO timestamps
VALUES (
        1,
        '2019-01-01 01:01:01',
        '2019-01-01 01:01:01.1',
        '2019-01-01 01:01:01.12',
        '2019-01-01 01:01:01.123',
        '2019-01-01 01:01:01.1234',
        '2019-01-01 01:01:01.12345',
        '2019-01-01 01:01:01.123456',
        '2019-01-01 01:01:01',
        '2019-01-01 01:01:01.1',
        '2019-01-01 01:01:01.12',
        '2019-01-01 01:01:01.123',
        '2019-01-01 01:01:01.1234',
        '2019-01-01 01:01:01.12345',
        '2019-01-01 01:01:01.123456'
);

INSERT INTO timestamps
VALUES (
           2,
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01',
           '2019-01-01 01:01:01'
       );