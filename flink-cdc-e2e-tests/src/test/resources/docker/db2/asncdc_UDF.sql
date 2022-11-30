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

DROP SPECIFIC FUNCTION ASNCDC.asncdcservice;

CREATE FUNCTION ASNCDC.ASNCDCSERVICES(command VARCHAR(6), service VARCHAR(8))
   RETURNS CLOB(100K)
   SPECIFIC asncdcservice
   EXTERNAL NAME 'asncdc!asncdcservice'
   LANGUAGE C
   PARAMETER STYLE SQL
   DBINFO
   DETERMINISTIC
   NOT FENCED
   RETURNS NULL ON NULL INPUT
   NO SQL
   NO EXTERNAL ACTION
   NO SCRATCHPAD
   ALLOW PARALLEL
   NO FINAL CALL;
