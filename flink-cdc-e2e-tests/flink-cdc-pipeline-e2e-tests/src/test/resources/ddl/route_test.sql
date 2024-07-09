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

DROP TABLE IF EXISTS TABLEALPHA;

CREATE TABLE TABLEALPHA (
    ID INT NOT NULL PRIMARY KEY,
    VERSION VARCHAR(17)
);

INSERT INTO TABLEALPHA VALUES (1008, '8');
INSERT INTO TABLEALPHA VALUES (1009, '8.1');
INSERT INTO TABLEALPHA VALUES (1010, '10');
INSERT INTO TABLEALPHA VALUES (1011, '11');

DROP TABLE IF EXISTS TABLEBETA;

CREATE TABLE TABLEBETA (
    ID INT NOT NULL PRIMARY KEY,
    VERSION VARCHAR(17)
);

INSERT INTO TABLEBETA VALUES (2011, '11');
INSERT INTO TABLEBETA VALUES (2012, '12');
INSERT INTO TABLEBETA VALUES (2013, '13');
INSERT INTO TABLEBETA VALUES (2014, '14');

DROP TABLE IF EXISTS TABLEGAMMA;

CREATE TABLE TABLEGAMMA (
    ID INT NOT NULL PRIMARY KEY,
    VERSION VARCHAR(17)
);

INSERT INTO TABLEGAMMA VALUES (3015, 'Amber');
INSERT INTO TABLEGAMMA VALUES (3016, 'Black');
INSERT INTO TABLEGAMMA VALUES (3017, 'Cyan');
INSERT INTO TABLEGAMMA VALUES (3018, 'Denim');

DROP TABLE IF EXISTS TABLEDELTA;

CREATE TABLE TABLEDELTA (
    ID INT NOT NULL PRIMARY KEY,
    VERSION VARCHAR(17)
);

INSERT INTO TABLEDELTA VALUES (4019, 'Yosemite');
INSERT INTO TABLEDELTA VALUES (4020, 'El Capitan');
INSERT INTO TABLEDELTA VALUES (4021, 'Sierra');
INSERT INTO TABLEDELTA VALUES (4022, 'High Sierra');
INSERT INTO TABLEDELTA VALUES (4023, 'Mojave');
INSERT INTO TABLEDELTA VALUES (4024, 'Catalina');