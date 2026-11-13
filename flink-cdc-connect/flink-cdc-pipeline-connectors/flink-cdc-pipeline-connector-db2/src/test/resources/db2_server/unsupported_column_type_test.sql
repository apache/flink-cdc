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
-- DATABASE:  unsupported_column_type_test
-- ----------------------------------------------------------------------------------------------------------------

CREATE TABLE DB2INST1.UNSUPPORTED_TYPES (
    ID INTEGER NOT NULL,
    DECFLOAT16_C DECFLOAT(16),
    DECFLOAT34_C DECFLOAT(34),
    XML_C XML,
    CLOB_C CLOB(200),
    DBCLOB_C DBCLOB(200),
    BLOB_C BLOB(100),
    PRIMARY KEY (ID)
);

INSERT INTO DB2INST1.UNSUPPORTED_TYPES VALUES (
    1, 1234.5678, 1234.5678, XMLPARSE(DOCUMENT '<a>abc</a>'),
    'clob payload', DBCLOB('dbclob payload'), BLOB('blob payload'));

VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');

CALL ASNCDC.ADDTABLE('DB2INST1', 'UNSUPPORTED_TYPES');

VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
