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

if [ ! -f /asncdctools/src/asncdc.nlk ]; then
rc=1
echo "waiting for db2inst1 exists ."
while [ "$rc" -ne 0 ]
do
   sleep 5
   id db2inst1
   rc=$?
   echo '.'
done

su  -c "/asncdctools/src/dbsetup.sh $DBNAME"   - db2inst1
fi
touch /asncdctools/src/asncdc.nlk

echo "The asncdc program enable finished"
