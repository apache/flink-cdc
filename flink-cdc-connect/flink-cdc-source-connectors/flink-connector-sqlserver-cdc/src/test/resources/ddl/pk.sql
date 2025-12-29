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

CREATE DATABASE pk;

USE pk;

-- Avoid SqlServer error com.microsoft.sqlserver.jdbc.SQLServerException: Could not update the metadata that indicates,
-- the root cause is 14258: 'Cannot perform this operation while SQLServerAgent is starting. Try again later.'. We simply
-- wait for 3 seconds to improve the test stabilises.
WAITFOR DELAY '00:00:03';
EXEC sys.sp_cdc_enable_db;

CREATE TABLE dt_pk (
    dt  datetime NOT NULL PRIMARY KEY,
    val INT
);

EXEC sys.sp_cdc_enable_table @source_schema = 'dbo', @source_name = 'dt_pk', @role_name = NULL, @supports_net_changes = 0;
