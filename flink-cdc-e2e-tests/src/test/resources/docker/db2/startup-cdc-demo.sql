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

VALUES ASNCDC.ASNCDCSERVICES('status','asncdc');

CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS');
CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS1');
CALL ASNCDC.ADDTABLE('DB2INST1', 'PRODUCTS2');
CALL ASNCDC.ADDTABLE('DB2INST1', 'FULL_TYPES');

VALUES ASNCDC.ASNCDCSERVICES('reinit','asncdc');
