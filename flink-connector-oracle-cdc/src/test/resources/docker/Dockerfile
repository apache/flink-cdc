################################################################################
#  Copyright 2022 Ververica Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
FROM wnameless/oracle-xe-11g-r2
ADD assets/ /assets/

ENV ORACLE_SID=XE ORACLE_HOME=/u01/app/oracle/product/11.2.0/xe

RUN mkdir /u01/app/oracle/oradata/recovery_area && \
    bash -x /assets/activate-archivelog.sh
