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
-- DATABASE:  user_1
-- ----------------------------------------------------------------------------------------------------------------

-- Create user_table_1_1 table
CREATE TABLE user_table_1_1 (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    address VARCHAR(1024),
    phone_number VARCHAR(512),
    email VARCHAR(255)
);

-- Create user_table_1_2 table
CREATE TABLE user_table_1_2 (
    id INTEGER NOT NULL PRIMARY KEY,
    name VARCHAR(255) NOT NULL DEFAULT 'flink',
    address VARCHAR(1024),
    phone_number VARCHAR(512)
);

INSERT INTO user_table_1_1
VALUES (111,"user_111","Shanghai","123567891234","user_111@foo.com");

INSERT INTO user_table_1_2
VALUES (121,"user_121","Shanghai","123567891234");