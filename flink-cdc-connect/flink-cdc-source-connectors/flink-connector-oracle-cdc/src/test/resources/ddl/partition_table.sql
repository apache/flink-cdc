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
-- DATABASE:  partition table test
-- ----------------------------------------------------------------------------------------------------------------

-- Create a range-partitioned table for CDC partition table testing
CREATE TABLE DEBEZIUM.PARTITIONED_CUSTOMERS (
  ID NUMBER(10) NOT NULL,
  NAME VARCHAR2(255) NOT NULL,
  ADDRESS VARCHAR2(1024),
  PHONE_NUMBER VARCHAR2(512),
  PRIMARY KEY(ID)
)
PARTITION BY RANGE (ID) (
  PARTITION p1 VALUES LESS THAN (500),
  PARTITION p2 VALUES LESS THAN (1000),
  PARTITION p3 VALUES LESS THAN (2000),
  PARTITION p4 VALUES LESS THAN (MAXVALUE)
);

ALTER TABLE DEBEZIUM.PARTITIONED_CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (101,'user_1','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (102,'user_2','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (103,'user_3','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (109,'user_4','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (110,'user_5','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (111,'user_6','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (501,'user_7','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (502,'user_8','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (513,'user_9','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1009,'user_10','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1010,'user_11','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1011,'user_12','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1012,'user_13','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1013,'user_14','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1014,'user_15','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1015,'user_16','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1016,'user_17','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1017,'user_18','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1018,'user_19','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (1019,'user_20','Shanghai','123567891234');
INSERT INTO DEBEZIUM.PARTITIONED_CUSTOMERS VALUES (2000,'user_21','Shanghai','123567891234');
