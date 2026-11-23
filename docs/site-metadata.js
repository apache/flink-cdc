/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module.exports = {
  version: '3.8-SNAPSHOT',
  versionTitle: '3.8-SNAPSHOT',
  branch: 'master',
  isStable: false,
  showOutDatedWarning: false,
  repository: 'https://github.com/apache/flink-cdc',
  previousDocs: ['3.7', '3.6', '3.5', '3.4', '3.3', '3.2', '3.1'].map(
    (version) => ({
      label: version,
      href: `https://nightlies.apache.org/flink/flink-cdc-docs-release-${version}/`,
    }),
  ),
};
