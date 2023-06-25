-- Copyright 2023 Ververica Inc.
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
-- DATABASE:  customer3.0
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our customers using a single insert with many rows
CREATE TABLE `customers3.0` (
  id INTEGER NOT NULL PRIMARY KEY,
  name VARCHAR(255) NOT NULL DEFAULT 'flink',
  address VARCHAR(1024),
  phone_number VARCHAR(512)
);

INSERT INTO `customers3.0`
VALUES (101,"user_1","Shanghai","123567891234"),
       (102,"user_2","Shanghai","123567891234"),
       (103,"user_3","Shanghai","123567891234"),
       (104,"user_4","Shanghai","123567891234");
