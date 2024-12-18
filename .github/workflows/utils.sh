#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

function random_timezone() {
    local rnd=$(expr $RANDOM % 25)
    local hh=$(expr $rnd / 2)
    local mm=$(expr $rnd % 2 \* 3)"0"
    local sgn=$(expr $RANDOM % 2)
    if [ $sgn -eq 0 ]
    then
        echo "GMT+$hh:$mm"
    else
        echo "GMT-$hh:$mm"
    fi
}