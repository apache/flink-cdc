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
FROM ibmcom/db2:11.5.0.0a

MAINTAINER  Peter Urbanetz

RUN mkdir -p /asncdctools/src

ADD asncdc_UDF.sql /asncdctools/src
ADD asncdcaddremove.sql /asncdctools/src
ADD asncdctables.sql /asncdctools/src
ADD dbsetup.sh /asncdctools/src
ADD startup-agent.sql /asncdctools/src
ADD startup-cdc-demo.sql /asncdctools/src
ADD inventory.sql /asncdctools/src
ADD column_type_test.sql /asncdctools/src
ADD asncdc.c /asncdctools/src

RUN chmod -R  777  /asncdctools

RUN mkdir /var/custom
ADD cdcsetup.sh /var/custom
RUN chmod -R  777 /var/custom
