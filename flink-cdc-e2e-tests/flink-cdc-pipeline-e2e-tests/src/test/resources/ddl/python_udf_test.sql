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

-- Fixture for PythonUdfE2eITCase. Rows are intentionally messy (mixed case,
-- whitespace, varied phone formats) so the Python UDFs have something to do
-- that built-in transform expressions can't easily match.

DROP TABLE IF EXISTS USERS;

CREATE TABLE USERS (
    ID INT NOT NULL,
    EMAIL VARCHAR(128),
    PHONE VARCHAR(64),
    AGE INT,
    SCORE DOUBLE,
    ACTIVE BOOLEAN,
    AVATAR VARBINARY(64),
    PRIMARY KEY (ID)
);

INSERT INTO USERS VALUES (1, 'Alice@Example.COM',  '+1 (415) 555-0100', 28, 87.5, TRUE,  X'48656C6C6F');
INSERT INTO USERS VALUES (2, '  bob@TEST.io  ',    '(212)-555-0199',    25, 64.0, FALSE, X'576F726C64');
INSERT INTO USERS VALUES (3, 'carol@example.com',  '+44 20 7946 0958',  35, 92.5, TRUE,  X'00FF7F');
INSERT INTO USERS VALUES (4, 'DAVE@example.org',   '+1.617.555.0173',   42, 78.0, TRUE,  X'CAFEBABE');
INSERT INTO USERS VALUES (5, NULL,                 NULL,                NULL, NULL, NULL,  NULL);
