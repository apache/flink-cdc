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
-- DATABASE:  mysql_case_inventory_with_comments
-- Schema-change matrix: comment preservation / modification / removal across column case variants.
-- All columns carry initial comments so that we can validate the comment-related path of
-- ALTER COLUMN TYPE / ALTER COLUMN COMMENT / column rename / column drop.
-- ----------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS student;
CREATE TABLE student (
  `id` BIGINT NOT NULL PRIMARY KEY COMMENT 'student id',
  `name` VARCHAR(64) NOT NULL COMMENT 'student name',
  `JOB` VARCHAR(64) COMMENT 'old job',
  `create_time` TIMESTAMP,
  `AGE` INT DEFAULT 30 COMMENT 'old age comment',
  `flag` INT DEFAULT 1
);

INSERT INTO student
VALUES (1, 'Alice', 'engineer', '2024-01-01 00:00:00', 18, 100),
       (2, 'Bob',   'doctor',   '2024-01-02 00:00:00', 19, 200);