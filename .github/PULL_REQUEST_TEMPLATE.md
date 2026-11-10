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

## What is the purpose of this pull request?

Briefly describe the problem this PR fixes or the feature it introduces. Reference Flink JIRA ticket when possible.

## Brief change log

List the key changes in this PR. 

## Flink version-sensitive API check

<!--
Flink CDC supports multiple Flink generations through the compat layer under
`flink-cdc-flink/`. Version-sensitive Flink API usage must be routed through that
compat layer rather than referenced directly from main code. Existing imports of the
shadowed compat classes are not affected by this checklist.
-->

- [ ] This PR adds no direct import of a version-sensitive `org.apache.flink.*` class (a class beyond the shadowed compat class list that is added, removed, or changed between Flink versions) and no reflection into `org.apache.flink` internals (fields, constructors, or methods).
- [ ] Any such usage added by this PR is routed through an Adapter in `flink-cdc-flink/flink1-common` or `flink-cdc-flink2-common`, or carried by a thin module (`flink-cdc-flink-1.20` / `flink-cdc-flink-2.2`).

---

## Verifying this change

<!-- Describe how this change can be verified. -->

This change is a trivial rework / code cleanup without any test coverage.

*(or)*

This change added tests and can be verified as follows:

- *Added/Updated unit tests in `...*`*
- *Added/Updated integration tests in `...*`*
- *Manually tested by ...*

## Documentation

- Does this pull request introduce a new feature? (yes / no)
- If yes, how is the feature documented? (not applicable / docs / JavaDocs / not documented)

---

##### Was generative AI tooling used to co-author this PR?

<!--
If generative AI tooling has been used in the process of authoring this PR, please
change the checkbox below to `[X]` followed by the name of the tool, and uncomment the
"Generated-by" line. See the ASF Generative Tooling Guidance for details:
https://www.apache.org/legal/generative-tooling.html

You are responsible for the quality and correctness of every change in this PR
regardless of the tooling used. Low-effort AI-generated PRs will be closed.
-->

- [ ] Yes (please specify the tool below)

<!--
Generated-by: [Tool Name and Version]
-->
