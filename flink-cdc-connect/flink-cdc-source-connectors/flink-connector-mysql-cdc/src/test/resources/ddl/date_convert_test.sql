-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the `License`); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an `AS IS` BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  column_type_test
-- ----------------------------------------------------------------------------------------------------------------

-- datetime NOT NULL

CREATE TABLE date_convert_test
(
    `id`                bigint NOT NULL AUTO_INCREMENT,
    `test_timestamp`    timestamp NULL,
    `test_datetime`     datetime NULL,
    `test_date`         date DEFAULT NULL,
    `test_time`         time DEFAULT NULL,
    PRIMARY KEY (`id`)
) DEFAULT CHARSET=utf8;

INSERT INTO date_convert_test (id,test_timestamp, test_datetime, test_date, test_time)
VALUES
(1,'2023-04-01 14:23:00', '2023-04-01 14:24:00', '2023-04-01', '14:25:00'),
(2,'2024-04-23 00:00:00', DEFAULT, NULL ,'00:00:00'),
(3,'2024-04-23 00:00:00', DEFAULT, NULL ,120),
(4,20240612150400, DEFAULT, NULL ,110);