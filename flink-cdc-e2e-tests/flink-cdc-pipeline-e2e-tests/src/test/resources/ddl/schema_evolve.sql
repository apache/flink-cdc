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

DROP TABLE IF EXISTS members;

CREATE TABLE members (
    id INT NOT NULL,
    name VARCHAR(17),
    age INT,
    PRIMARY KEY (id)
);

INSERT INTO members VALUES (1008, 'Alice', 21);
INSERT INTO members VALUES (1009, 'Bob', 20);
INSERT INTO members VALUES (1010, 'Carol', 19);
INSERT INTO members VALUES (1011, 'Derrida', 18);

DROP TABLE IF EXISTS new_members;

CREATE TABLE new_members (
    id INT NOT NULL,
    name VARCHAR(17),
    age INT,
    PRIMARY KEY (id)
);
