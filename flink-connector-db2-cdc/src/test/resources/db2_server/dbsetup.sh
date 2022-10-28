#/bin/bash
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
#  limitations under the License.
################################################################################

echo "Compile ASN tool ..."
cd /asncdctools/src
/opt/ibm/db2/V11.5/samples/c/bldrtn asncdc

DBNAME=$1
DB2DIR=/opt/ibm/db2/V11.5
rc=1
echo "Waiting for DB2 start ( $DBNAME ) ."
while [ "$rc" -ne 0 ]
do
   sleep 5
   db2 connect to $DBNAME
   rc=$?
   echo '.'
done

# enable metacatalog read via JDBC
cd $HOME/sqllib/bnd
db2 bind db2schema.bnd blocking all grant public sqlerror continue 

# do a backup and restart the db
db2 backup db $DBNAME to /dev/null
db2 restart db $DBNAME

db2 connect to $DBNAME

cp /asncdctools/src/asncdc /database/config/db2inst1/sqllib/function
chmod 777 /database/config/db2inst1/sqllib/function

# add UDF / start stop asncap
db2 -tvmf /asncdctools/src/asncdc_UDF.sql

# create asntables
db2 -tvmf /asncdctools/src/asncdctables.sql

# add UDF / add remove asntables

db2 -tvmf /asncdctools/src/asncdcaddremove.sql




# create sample table and data
db2 -tvmf /asncdctools/src/inventory.sql
db2 -tvmf /asncdctools/src/column_type_test.sql
db2 -tvmf /asncdctools/src/startup-agent.sql
sleep 10
db2 -tvmf /asncdctools/src/startup-cdc-demo.sql 




echo "db2 setup done"
