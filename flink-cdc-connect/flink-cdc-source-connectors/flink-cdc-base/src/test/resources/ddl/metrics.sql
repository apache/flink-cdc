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
-- DATABASE:  metrics
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE users (
  id INTEGER NOT NULL AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255),
  age integer
);
ALTER TABLE users AUTO_INCREMENT = 101;

INSERT INTO users
VALUES (default,"Tom",3),
       (default,"Jack",5),
       (default,"Allen",10),
       (default,"Andrew",13),
       (default,"Arnold",15),
       (default,"Claud",19),
       (default,"Howard",37),
       (default,"Jacob",46),
       (default,"Lionel",58);