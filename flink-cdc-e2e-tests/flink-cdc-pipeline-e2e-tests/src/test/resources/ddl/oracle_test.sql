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
-- DATABASE:  customer
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate products and category tables using a single insert with many rows

CREATE TABLE DEBEZIUM.PRODUCTS (
  ID NUMBER(9, 0) NOT NULL,
  NAME VARCHAR(255) NOT NULL,
  DESCRIPTION VARCHAR(512),
  WEIGHT FLOAT,
  PRIMARY KEY(ID)
);


ALTER TABLE DEBEZIUM.PRODUCTS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (101,'scooter','Small 2-wheel scooter',3.14);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (102,'car battery','12V car battery',8.1);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (103,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',1.8);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (104,'hammer','12oz carpenters hammer',0.75);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (105,'hammer','14oz carpenters hammer',0.875);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (106,'hammer','16oz carpenters hammer',1);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (107,'rocks','box of assorted rocks',5.3);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (108,'jacket','water resistent black wind breaker',0.1);
INSERT INTO DEBEZIUM.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (109,'spare tire','24 inch spare tire',22.2);

CREATE TABLE DEBEZIUM.CUSTOMERS (
                                    ID INT NOT NULL,
                                    NAME VARCHAR2(255) NOT NULL,
                                    ADDRESS VARCHAR2(1024),
                                    PHONE_NUMBER VARCHAR2(512),
                                    PRIMARY KEY(ID)
);

ALTER TABLE DEBEZIUM.CUSTOMERS ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

INSERT INTO DEBEZIUM.CUSTOMERS VALUES (101,'user_1','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (102,'user_2','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (103,'user_3','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (109,'user_4','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (110,'user_5','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (111,'user_6','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (118,'user_7','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (121,'user_8','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (123,'user_9','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1009,'user_10','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1010,'user_11','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1011,'user_12','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1012,'user_13','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1013,'user_14','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1014,'user_15','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1015,'user_16','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1016,'user_17','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1017,'user_18','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1018,'user_19','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1019,'user_20','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS VALUES (2000,'user_21','Shanghai','123567891234');
--
-- -- table has same name prefix with 'customers.*'
CREATE TABLE DEBEZIUM.CUSTOMERS_1 (
                                      ID INT NOT NULL,
                                      NAME VARCHAR2(255) NOT NULL,
                                      ADDRESS VARCHAR2(1024),
                                      PHONE_NUMBER VARCHAR2(512),
                                      PRIMARY KEY(ID)
);
ALTER TABLE DEBEZIUM.CUSTOMERS_1 ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (101,'user_1','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (102,'user_2','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (103,'user_3','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (109,'user_4','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (110,'user_5','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (111,'user_6','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (118,'user_7','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (121,'user_8','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (123,'user_9','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1009,'user_10','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1010,'user_11','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1011,'user_12','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1012,'user_13','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1013,'user_14','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1014,'user_15','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1015,'user_16','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1016,'user_17','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1017,'user_18','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1018,'user_19','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1019,'user_20','Shanghai','123567891234');
INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (2000,'user_21','Shanghai','123567891234');