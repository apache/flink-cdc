/*
<!--
Copyright 2022 Ververica Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/
prompt Shutting down database to activate archivelog mode;
shutdown immediate;
startup mount;
alter database archivelog;
prompt Archive log activated.;
alter database add supplemental log data (all) columns;
prompt Activated supplemental logging with all columns.;
prompt Starting up database;
alter database open;

CREATE TABLESPACE LOGMINER_TBS DATAFILE '/u01/app/oracle/oradata/XE/logminer_tbs.dbf' SIZE 25M REUSE AUTOEXTEND ON MAXSIZE UNLIMITED;
CREATE USER dbzuser IDENTIFIED BY dbz DEFAULT TABLESPACE LOGMINER_TBS QUOTA UNLIMITED ON LOGMINER_TBS;
CREATE USER debezium IDENTIFIED BY dbz DEFAULT TABLESPACE USERS QUOTA UNLIMITED ON USERS;

ALTER DATABASE ADD SUPPLEMENTAL LOG DATA;

CREATE TABLE debezium.products (
  ID NUMBER(9, 0) NOT NULL,
  NAME VARCHAR(255) NOT NULL,
  DESCRIPTION VARCHAR(512),
  WEIGHT FLOAT,
  PRIMARY KEY(ID)
);
CREATE TABLE debezium.category (
  ID NUMBER(9, 0) NOT NULL,
  CATEGORY_NAME VARCHAR(255),
  PRIMARY KEY(ID)
);

ALTER TABLE debezium.products ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER TABLE debezium.category ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
ALTER USER debezium QUOTA 100M on users;

GRANT CREATE SESSION TO dbzuser;
GRANT SELECT ON V_$DATABASE TO dbzuser;
GRANT FLASHBACK ANY TABLE TO dbzuser;
GRANT SELECT ANY TABLE TO dbzuser;
GRANT SELECT_CATALOG_ROLE TO dbzuser;
GRANT EXECUTE_CATALOG_ROLE TO dbzuser;
GRANT SELECT ANY TRANSACTION TO dbzuser;
GRANT SELECT ANY DICTIONARY TO dbzuser;
GRANT INSERT ANY TABLE TO dbzuser;
GRANT UPDATE ANY TABLE TO dbzuser;
GRANT DELETE ANY TABLE TO dbzuser;

GRANT CREATE ANY TABLE TO dbzuser;
GRANT ALTER ANY TABLE TO dbzuser;
GRANT LOCK ANY TABLE TO dbzuser;
GRANT CREATE SEQUENCE TO dbzuser;

GRANT EXECUTE ON DBMS_LOGMNR TO dbzuser;
GRANT EXECUTE ON DBMS_LOGMNR_D TO dbzuser;
GRANT SELECT ON V_$LOGMNR_LOGS to dbzuser;
GRANT SELECT ON V_$LOGMNR_CONTENTS TO dbzuser;
GRANT SELECT ON V_$LOGFILE TO dbzuser;
GRANT SELECT ON V_$ARCHIVED_LOG TO dbzuser;
GRANT SELECT ON V_$ARCHIVE_DEST_STATUS TO dbzuser;
GRANT SELECT ON V_$LOGMNR_PARAMETERS TO dbzuser;
GRANT SELECT ON V_$LOG TO dbzuser;
GRANT SELECT ON V_$LOG_HISTORY TO dbzuser;

GRANT CONNECT TO debezium;
GRANT CREATE SESSION TO debezium;
GRANT CREATE TABLE TO debezium;
GRANT CREATE SEQUENCE to debezium;


INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (101,'scooter','Small 2-wheel scooter',3.14);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (102,'car battery','12V car battery',8.1);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (103,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (104,'hammer','12oz carpenters hammer',0.75);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (105,'hammer','14oz carpenters hammer',0.875);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (106,'hammer','16oz carpenters hammer',1.0);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (107,'rocks','box of assorted rocks',5.3);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (108,'jacket','water resistent black wind breaker',0.1);
INSERT INTO debezium.PRODUCTS (ID,NAME,DESCRIPTION,WEIGHT)
    VALUES (109,'spare tire','24 inch spare tire',22.2);
