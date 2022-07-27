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
-- DATABASE:  composite_pk
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE composite_pk_table (
  nickname VARCHAR(255) NOT NULL,
  id BIGINT NOT NULL,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(1024),
  phone_number VARCHAR(512),
  PRIMARY KEY (`nickname`, `id`,`name`)
);

INSERT INTO composite_pk_table
VALUES ("a", 1, "name1", "Beijing", "0123"),
       ("b", 2, "name1", "Beijing", "4567"),
       ("c", 3, "name1", "Beijing", "8901"),
       ("d", 4, "name2", "Beijing", "2345"),
       ("e", 5, "name2", "Beijing", "6789"),
       ("f", 6, "name2", "Beijing", "0123"),
       ("a", 7, "name3", "Beijing", "4567"),
       ("b", 8, "name3", "Beijing", "8901"),
       ("c", 9, "name3", "Beijing", "2345"),
       ("d", 10, "name4", "Beijing", "6789"),
       ("e", 11, "name4", "Beijing", "0123"),
       ("f", 12, "name4", "Beijing", "0123"),
       ("a", 13, "name5", "Beijing", "0123"),
       ("b", 14, "name5", "Beijing", "0123"),
       ("c", 15, "name5", "Beijing", "0123");