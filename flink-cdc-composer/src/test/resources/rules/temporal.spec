################################################################################
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

-------------------------------------
           do Date Format Function (UTC)
   projection id_
            | DATE_FORMAT(timestamp_0_, 'yyyy~MM~dd') AS comp_1
            | DATE_FORMAT(timestamp_6_, 'yyyy~MM~dd') AS comp_2
            | DATE_FORMAT(timestamp_9_, 'yyyy~MM~dd') AS comp_3
            | DATE_FORMAT(timestamp_ltz_0_, 'yyyy~MM~dd') AS comp_4
            | DATE_FORMAT(timestamp_ltz_6_, 'yyyy~MM~dd') AS comp_5
            | DATE_FORMAT(timestamp_ltz_9_, 'yyyy~MM~dd') AS comp_6
            | DATE_FORMAT(timestamp_0_, 'yyyy->MM->dd HH->mm->ss') AS comp_7
            | DATE_FORMAT(timestamp_6_, 'yyyy->MM->dd HH->mm->ss') AS comp_8
            | DATE_FORMAT(timestamp_9_, 'yyyy->MM->dd HH->mm->ss') AS comp_9
            | DATE_FORMAT(timestamp_ltz_0_, 'yyyy->MM->dd HH->mm->ss') AS comp_10
            | DATE_FORMAT(timestamp_ltz_6_, 'yyyy->MM->dd HH->mm->ss') AS comp_11
            | DATE_FORMAT(timestamp_ltz_9_, 'yyyy->MM->dd HH->mm->ss') AS comp_12
  primary-key id_
       expect CreateTableEvent{tableId=foo.bar.baz, schema=columns={`id_` BIGINT NOT NULL 'Identifier',`comp_1` STRING,`comp_2` STRING,`comp_3` STRING,`comp_4` STRING,`comp_5` STRING,`comp_6` STRING,`comp_7` STRING,`comp_8` STRING,`comp_9` STRING,`comp_10` STRING,`comp_11` STRING,`comp_12` STRING}, primaryKeys=id_, options=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[1, 1970~01~02, 1970~01~03, 1970~01~05, 1970~01~02, 1970~01~03, 1970~01~05, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[1, 1970~01~02, 1970~01~03, 1970~01~05, 1970~01~02, 1970~01~03, 1970~01~05, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18], after=[-1, 1970~01~09, 1970~01~10, 1970~01~11, 1970~01~09, 1970~01~10, 1970~01~11, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18], op=UPDATE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[-1, 1970~01~09, 1970~01~10, 1970~01~11, 1970~01~09, 1970~01~10, 1970~01~11, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18], after=[], op=DELETE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[0, null, null, null, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[0, null, null, null, null, null, null, null, null, null, null, null, null], after=[], op=DELETE, meta=()}
-------------------------------------
           do Date Format Function (Asia/Shanghai)
   projection id_
            | DATE_FORMAT(timestamp_0_, 'yyyy~MM~dd') AS comp_1
            | DATE_FORMAT(timestamp_6_, 'yyyy~MM~dd') AS comp_2
            | DATE_FORMAT(timestamp_9_, 'yyyy~MM~dd') AS comp_3
            | DATE_FORMAT(timestamp_ltz_0_, 'yyyy~MM~dd') AS comp_4
            | DATE_FORMAT(timestamp_ltz_6_, 'yyyy~MM~dd') AS comp_5
            | DATE_FORMAT(timestamp_ltz_9_, 'yyyy~MM~dd') AS comp_6
            | DATE_FORMAT(timestamp_0_, 'yyyy->MM->dd HH->mm->ss') AS comp_7
            | DATE_FORMAT(timestamp_6_, 'yyyy->MM->dd HH->mm->ss') AS comp_8
            | DATE_FORMAT(timestamp_9_, 'yyyy->MM->dd HH->mm->ss') AS comp_9
            | DATE_FORMAT(timestamp_ltz_0_, 'yyyy->MM->dd HH->mm->ss') AS comp_10
            | DATE_FORMAT(timestamp_ltz_6_, 'yyyy->MM->dd HH->mm->ss') AS comp_11
            | DATE_FORMAT(timestamp_ltz_9_, 'yyyy->MM->dd HH->mm->ss') AS comp_12
  primary-key id_
    time-zone Asia/Shanghai
       expect CreateTableEvent{tableId=foo.bar.baz, schema=columns={`id_` BIGINT NOT NULL 'Identifier',`comp_1` STRING,`comp_2` STRING,`comp_3` STRING,`comp_4` STRING,`comp_5` STRING,`comp_6` STRING,`comp_7` STRING,`comp_8` STRING,`comp_9` STRING,`comp_10` STRING,`comp_11` STRING,`comp_12` STRING}, primaryKeys=id_, options=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[1, 1970~01~02, 1970~01~03, 1970~01~05, 1970~01~02, 1970~01~04, 1970~01~05, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18, 1970->01->02 18->17->36, 1970->01->04 01->09->27, 1970->01->05 08->01->18], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[1, 1970~01~02, 1970~01~03, 1970~01~05, 1970~01~02, 1970~01~04, 1970~01~05, 1970->01->02 10->17->36, 1970->01->03 17->09->27, 1970->01->05 00->01->18, 1970->01->02 18->17->36, 1970->01->04 01->09->27, 1970->01->05 08->01->18], after=[-1, 1970~01~09, 1970~01~10, 1970~01~11, 1970~01~09, 1970~01~10, 1970~01~12, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18, 1970->01->09 16->57->36, 1970->01->10 23->49->27, 1970->01->12 06->41->18], op=UPDATE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[-1, 1970~01~09, 1970~01~10, 1970~01~11, 1970~01~09, 1970~01~10, 1970~01~12, 1970->01->09 08->57->36, 1970->01->10 15->49->27, 1970->01->11 22->41->18, 1970->01->09 16->57->36, 1970->01->10 23->49->27, 1970->01->12 06->41->18], after=[], op=DELETE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[0, null, null, null, null, null, null, null, null, null, null, null, null], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[0, null, null, null, null, null, null, null, null, null, null, null, null], after=[], op=DELETE, meta=()}
-------------------------------------
           do Timestamp Diff Function
   projection id_
            | TIMESTAMP_DIFF('SECOND', timestamp_0_, timestamp_0_) AS comp_1
            | TIMESTAMP_DIFF('MINUTE', timestamp_6_, timestamp_0_) AS comp_2
            | TIMESTAMP_DIFF('HOUR', timestamp_9_, timestamp_0_) AS comp_3
            | TIMESTAMP_DIFF('DAY', timestamp_ltz_0_, timestamp_ltz_0_) AS comp_4
            | TIMESTAMP_DIFF('MONTH', timestamp_ltz_6_, timestamp_ltz_0_) AS comp_5
            | TIMESTAMP_DIFF('YEAR', timestamp_ltz_9_, timestamp_ltz_0_) AS comp_6
  primary-key id_
       expect CreateTableEvent{tableId=foo.bar.baz, schema=columns={`id_` BIGINT NOT NULL 'Identifier',`comp_1` INT,`comp_2` INT,`comp_3` INT,`comp_4` INT,`comp_5` INT,`comp_6` INT}, primaryKeys=id_, options=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[1, 0, -1851, -61, 0, 0, 0], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[1, 0, -1851, -61, 0, 0, 0], after=[-1, 0, -1851, -61, 0, 0, 0], op=UPDATE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[-1, 0, -1851, -61, 0, 0, 0], after=[], op=DELETE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[0, null, null, null, null, null, null], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[0, null, null, null, null, null, null], after=[], op=DELETE, meta=()}
-------------------------------------
           do To Date Function
   projection id_
            | TO_DATE('2025-01-05') AS comp_1
            | TO_DATE('9999-12-31') AS comp_2
            | TO_DATE('2024//02//14', 'yyyy//MM//dd') AS comp_3
            | TO_DATE('2024 !! 02 !! 14', 'yyyy//MM//dd') AS comp_4
  primary-key id_
       expect CreateTableEvent{tableId=foo.bar.baz, schema=columns={`id_` BIGINT NOT NULL 'Identifier',`comp_1` DATE,`comp_2` DATE,`comp_3` DATE,`comp_4` DATE}, primaryKeys=id_, options=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[1, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[1, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], after=[-1, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], op=UPDATE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[-1, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], after=[], op=DELETE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[0, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[0, 2025-01-05, 9999-12-31, 2024-02-14, +1705471-09-26], after=[], op=DELETE, meta=()}
-------------------------------------
           do To Timestamp Function
   projection id_
            | TO_TIMESTAMP('2025-01-05 19:59:59') AS comp_1
            | TO_TIMESTAMP('2024//02//14 23//44//17', 'yyyy//MM//dd HH//mm//ss') AS comp_2
  primary-key id_
       expect CreateTableEvent{tableId=foo.bar.baz, schema=columns={`id_` BIGINT NOT NULL 'Identifier',`comp_1` TIMESTAMP(3),`comp_2` TIMESTAMP(3)}, primaryKeys=id_, options=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[1, 2025-01-05T19:59:59, 2024-02-14T23:44:17], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[1, 2025-01-05T19:59:59, 2024-02-14T23:44:17], after=[-1, 2025-01-05T19:59:59, 2024-02-14T23:44:17], op=UPDATE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[-1, 2025-01-05T19:59:59, 2024-02-14T23:44:17], after=[], op=DELETE, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[], after=[0, 2025-01-05T19:59:59, 2024-02-14T23:44:17], op=INSERT, meta=()}
            | DataChangeEvent{tableId=foo.bar.baz, before=[0, 2025-01-05T19:59:59, 2024-02-14T23:44:17], after=[], op=DELETE, meta=()}
-------------------------------------
           do To Timestamp Function Failure (1)
   projection id_
            | TO_TIMESTAMP('2024 !! 02 !! 14', 'yyyy//MM//dd') AS comp
 expect-error Unparseable date: "2024 !! 02 !! 14"
 -------------------------------------
           do To Timestamp Function Failure (2)
   projection id_
            | TO_TIMESTAMP('2024 !! 02 !! 14 11 !! 45 !! 49', 'yyyy//MM//dd') AS comp
 expect-error Unparseable date: "2024 !! 02 !! 14 11 !! 45 !! 49"
