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

-- Create a table with complicated metadata to make sure the following
-- optional metadata parsing will not succeed unless the previous column
-- metadata has been correctly parsed.

CREATE TABLE `json_array_as_key` (
                                  `id` bigint(20) unsigned NOT NULL,
                                  `c2` bigint(20) NOT NULL DEFAULT '0',
                                  `c3` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
                                  `c4` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
                                  `c5` varchar(256) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
                                  `c6` varchar(1024) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
                                  `c7` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '',
                                  `c8` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
                                  `c9` int(11) DEFAULT '0',
                                  `c10` int(11) DEFAULT '0',
                                  `c11` int(11) NOT NULL DEFAULT '0',
                                  `c12` int(11) DEFAULT '0',
                                  `c13` json DEFAULT NULL,
                                  `c14` json DEFAULT NULL,
                                  `c15` json DEFAULT NULL,
                                  `c16` json DEFAULT NULL,
                                  `c17` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
                                  `c18` varchar(128) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT '',
                                  `c19` tinyint(4) DEFAULT '0',
                                  `c20` tinyint(4) NOT NULL DEFAULT '0',
                                  `c21` tinyint(4) NOT NULL DEFAULT '0',
                                  `c22` tinyint(4) NOT NULL DEFAULT '0',
                                  `c23` tinyint(4) DEFAULT '0',
                                  `c24` tinyint(3) unsigned NOT NULL DEFAULT '1',
                                  `c25` int(10) unsigned NOT NULL DEFAULT '0',
                                  `c26` int(10) unsigned NOT NULL DEFAULT '0',
                                  `c27` int(10) unsigned NOT NULL DEFAULT '0',
                                  `c28` int(10) unsigned NOT NULL DEFAULT '0',
                                  PRIMARY KEY (`id`),
                                  KEY `k6` ((cast(json_extract(`c13`,_utf8mb4'$[*]') as CHAR(32) ARRAY)))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO json_array_as_key(ID) VALUES (17);