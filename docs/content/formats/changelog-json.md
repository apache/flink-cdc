<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Changelog JSON Format

**WARNING:** The CDC format `changelog-json` is deprecated since Flink CDC version 2.2.
The CDC format `changelog-json` was introduced at the point that Flink didn't offer any CDC format. Currently, Flink offers several well-maintained CDC formats i.e.[Debezium CDC, MAXWELL CDC, CANAL CDC](https://nightlies.apache.org/flink/flink-docs-release-1.17/docs/connectors/table/formats/overview/), we recommend user to use above CDC formats.

### Compatibility Note

User can still obtain and use the deprecated `changelog-json` format from older Flink CDC version e.g. [flink-format-changelog-json-2.1.1.jar](https://repo1.maven.org/maven2/com/ververica/flink-format-changelog-json/2.1.1/flink-format-changelog-json-2.1.1-SNAPSHOT.jar).
