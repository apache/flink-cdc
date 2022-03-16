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

## This script is copied from https://github.com/docker/for-mac/issues/2359#issuecomment-991942550
#brew install --cask docker
#docker_app_path=$(brew list --cask docker | grep '==> App' -A1 | tail -n 1 | awk '{ print $1 }')
#docker_app_path="${docker_app_path/#\~/$HOME}"
#
#sudo "$docker_app_path"/Contents/MacOS/Docker --unattended --install-privileged-components
#open -a "$docker_app_path" --args --unattended --accept-license
#while ! "$docker_app_path"/Contents/Resources/bin/docker info &>/dev/null; do sleep 1; done

brew install docker colima
colima start --cpu 2 --memory 12