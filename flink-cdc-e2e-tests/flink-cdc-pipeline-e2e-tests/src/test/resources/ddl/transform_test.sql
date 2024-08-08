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
    ID INT NOT NULL,
    VERSION VARCHAR(17),
    PRICEALPHA INT,
    AGEALPHA INT,
    NAMEALPHA VARCHAR(128),
    PRIMARY KEY (ID)
);

INSERT INTO TABLEALPHA VALUES (1008, '8', 199, 17, 'Alice');
INSERT INTO TABLEALPHA VALUES (1009, '8.1', 0, 18, 'Bob');
INSERT INTO TABLEALPHA VALUES (1010, '10', 99, 19, 'Carol');
INSERT INTO TABLEALPHA VALUES (1011, '11', 59, 20, 'Dave');

DROP TABLE IF EXISTS TABLEBETA;

CREATE TABLE TABLEBETA (
    ID INT NOT NULL,
    VERSION VARCHAR(17),
    CODENAMESBETA VARCHAR(17),
    AGEBETA INT,
    NAMEBETA VARCHAR(128),
    PRIMARY KEY (ID)
);

INSERT INTO TABLEBETA VALUES (2011, '11', 'Big Sur', 21, 'Eva');
INSERT INTO TABLEBETA VALUES (2012, '12', 'Monterey', 22, 'Fred');
INSERT INTO TABLEBETA VALUES (2013, '13', 'Ventura', 23, 'Gus');
INSERT INTO TABLEBETA VALUES (2014, '14', 'Sonoma', 24, 'Henry');