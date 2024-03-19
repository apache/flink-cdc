---
title: "Contribute to Flink CDC"
weight: 2
type: docs
aliases:
  - /developer-guide/contribute-to-flink-cdc
---
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

# Contributing

Flink CDC is developed by an open and friendly community and welcomes anyone who wants to help out in any way.
There are several ways to interact with the community and contribute to Flink CDC including asking questions, filing 
bug reports, proposing new features, joining discussions on the mailing lists, contributing code or documentation, 
improving website, testing release candidates and writing corresponding blog etc.

## What do you want to do

Contributing to Flink CDC goes beyond writing code for the project. Here are different opportunities to help the 
project as follows.

| Area            | Further information                                                                                                                                                                                                                                                                                                                                 |
|:----------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Report Bug      | To report a problem, open an issue in [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) and select `Flink CDC` in `Component/s`. Please give detailed information about the problem you encountered and, if possible, add a description that helps to reproduce the problem.                                                       |
| Contribute Code | Read the <a href="#code-contribution-guide">Code Contribution Guide</a>                                                                                                                                                                                                                                                                             |
| Code Reviews    | Read the <a href="#code-review-guide">Code Review Guide</a>                                                                                                                                                                                                                                                                                         |
| Support Users   | Reply to questions on the [flink user mailing list](https://flink.apache.org/what-is-flink/community/#mailing-lists), check the latest issues in [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) for tickets which are actually user questions.                                                                                  |

Any other question? Reach out to the Dev mail list to get help!

<h2 id="code-review-guide">Code Contribution Guide</h2>

Flink CDC is maintained, improved, and extended by code contributions of volunteers. We welcome contributions.

Please feel free to ask questions at any time. Either send a mail to the Dev mailing list or comment on the issue you are working on.

If you would like to contribute to Flink CDC, you could raise it as follows.

1. Left comment under the issue that you want to take.  (It's better to explain your understanding of the issue, and 
   your design, and if possible, you need to provide your POC code)
2. Start to implement it after this issue is assigned to you. (Commit message should follow the format `[FLINK-xxx][xxx] xxxxxxx`)
3. Create a PR to [Flink CDC](https://github.com/apache/flink-cdc). (Please enable the actions of your own clone 
   project)
4. Find a reviewer to review your PR and make sure the CI passed
5. A committer of Flink CDC checks if the contribution fulfills the requirements and merges the code to the codebase.

<h2 id="code-contribution-guide">Code Review Guide</h2>

Every review needs to check the following aspects. 

1. Is This Pull Request Well-Described?

Check whether this pull request is sufficiently well-described to support a good review. Trivial changes and fixes do 
not need a long description.

2. How Is the Overall Code Quality, Meeting Standard We Want to Maintain? 

- Does the code follow the right software engineering practices? 
- Is the code correct, robust, maintainable, testable?
- Are the changes performance aware, when changing a performance sensitive part?
- Are the changes sufficiently covered by tests? Are the tests executing fast?
- If dependencies have been changed, were the NOTICE files updated?
- Does the commit message follow the required format?

3. Are the Documentation Updated?

If the pull request introduces a new feature, the feature should be documented.
