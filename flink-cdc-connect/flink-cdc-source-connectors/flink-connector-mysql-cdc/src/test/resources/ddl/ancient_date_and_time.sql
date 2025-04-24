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

CREATE TABLE ancient_times
(
    id             SERIAL,
    date_col       DATE,
    datetime_0_col DATETIME(0),
    datetime_1_col DATETIME(1),
    datetime_2_col DATETIME(2),
    datetime_3_col DATETIME(3),
    datetime_4_col DATETIME(4),
    datetime_5_col DATETIME(5),
    datetime_6_col DATETIME(6),
    PRIMARY KEY (id)
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0017-08-12',
    '0016-07-13 17:17:17',
    '0015-06-14 17:17:17.1',
    '0014-05-15 17:17:17.12',
    '0013-04-16 17:17:17.123',
    '0012-03-17 17:17:17.1234',
    '0011-02-18 17:17:17.12345',
    '0010-01-19 17:17:17.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0000-00-00',
    '0000-00-00 00:00:00',
    '0000-00-00 00:00:00.0',
    '0000-00-00 00:00:00.00',
    '0000-00-00 00:00:00.000',
    '0000-00-00 00:00:00.0000',
    '0000-00-00 00:00:00.00000',
    '0000-00-00 00:00:00.000000'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0001-01-01',
    '0001-01-01 16:16:16',
    '0001-01-01 16:16:16.1',
    '0001-01-01 16:16:16.12',
    '0001-01-01 16:16:16.123',
    '0001-01-01 16:16:16.1234',
    '0001-01-01 16:16:16.12345',
    '0001-01-01 16:16:16.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0002-02-02',
    '0002-02-02 15:15:15',
    '0002-02-02 15:15:15.1',
    '0002-02-02 15:15:15.12',
    '0002-02-02 15:15:15.123',
    '0002-02-02 15:15:15.1234',
    '0002-02-02 15:15:15.12345',
    '0002-02-02 15:15:15.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0033-03-03',
    '0033-03-03 14:14:14',
    '0033-03-03 14:14:14.1',
    '0033-03-03 14:14:14.12',
    '0033-03-03 14:14:14.123',
    '0033-03-03 14:14:14.1234',
    '0033-03-03 14:14:14.12345',
    '0033-03-03 14:14:14.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '0444-04-04',
    '0444-04-04 13:13:13',
    '0444-04-04 13:13:13.1',
    '0444-04-04 13:13:13.12',
    '0444-04-04 13:13:13.123',
    '0444-04-04 13:13:13.1234',
    '0444-04-04 13:13:13.12345',
    '0444-04-04 13:13:13.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '1969-12-31',
    '1969-12-31 12:12:12',
    '1969-12-31 12:12:12.1',
    '1969-12-31 12:12:12.12',
    '1969-12-31 12:12:12.123',
    '1969-12-31 12:12:12.1234',
    '1969-12-31 12:12:12.12345',
    '1969-12-31 12:12:12.123456'
);

INSERT INTO ancient_times VALUES (
    DEFAULT,
    '2019-12-31',
    '2019-12-31 23:11:11',
    '2019-12-31 23:11:11.1',
    '2019-12-31 23:11:11.12',
    '2019-12-31 23:11:11.123',
    '2019-12-31 23:11:11.1234',
    '2019-12-31 23:11:11.12345',
    '2019-12-31 23:11:11.123456'
);
