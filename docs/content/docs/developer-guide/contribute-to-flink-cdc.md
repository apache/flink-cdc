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

| Area                       | Further information                                                                                                                                                                                                                                                                           |
|:---------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Report Bug                 | To report a problem, open an issue in [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) and select `Flink CDC` in `Component/s`. Please give detailed information about the problem you encountered and, if possible, add a description that helps to reproduce the problem. |
| Contribute Code            | Read the <a href="#code-contribution-guide">Code Contribution Guide</a>                                                                                                                                                                                                                       |
| Code Reviews               | Read the <a href="#code-review-guide">Code Review Guide</a>                                                                                                                                                                                                                                   |
| Release Verification       | Read the <a href="#release-validation-guide">Code Review Guide</a>                                                                                                                                                                                                                            |
| Support Users              | Reply to questions on the [Flink user mailing list](https://flink.apache.org/what-is-flink/community/#mailing-lists), check the latest issues in [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) for tickets about user questions.                                         |
| Join Developer Discussions | Be informed with developers' discussions by subscribing to [Flink dev mailing list](https://lists.apache.org/list.html?dev@flink.apache.org).                                                                                                                                                 | 

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

<h2 id="release-validation-guide">Release Verification Guide</h2>

We will prepare new releases of Flink CDC regularly.

According to official [Apache Software Foundation Releasing policy](https://www.apache.org/legal/release-policy.html),
we will make a release candidate (RC) version before each release,
and invite community members to test and vote on this pre-release version.

Everyone is welcome to participate in the version verification work in `dev@flink.apache.org` mailing list.
The verification process may include the following aspects:

1. Verify if source code could be compiled successfully.

Currently, Flink CDC uses [Maven](https://maven.apache.org/) 3 as the build tool and compiles on the JDK 8 platform.
You can download the RC version of the source code package and compile it using the `mvn clean package -Dfast` command,
and check if there are any unexpected errors or warnings.

2. Verify if tarball checksum matches.

To ensure the genuinity and integrity of released binary packages, a SHA512 hash value of the corresponding file is attached to any released binary tarball so that users can verify the integrity.
You can download the binary tarball of the RC version and calculate its SHA512 hash value with the following command:

* Linux: `sha512sum flink-cdc-*-bin.tar.gz`
* macOS: `shasum -a 512 flink-cdc-*-bin.tar.gz`
* Windows (PowerShell): `Get-FileHash flink-cdc-*-bin.tar.gz -Algorithm SHA512 | Format-List`

To verify tarball signature, ensure that you have gpg (version 2.4+) is installed.

First, download the releasing PMC/Committer's GPG public key from KEYS location, 
and import one of the release manager's public key:

```bash
gpg --import <releaser-public-key>
```

For example:

```bash
gpg  --import leonard-xu.pub
```

The file leonard-xu.pub contains the @leonardBang 's public key from the [KEYS](https://dist.apache.org/repos/dist/release/flink/KEYS) list above.

Verify the tarball signature is genuine:

```bash
gpg --verify flink-cdc-<VERSION>-src.tgz.asc flink-cdc-<VERSION>-src.tgz
```

For example, verify version 3.1.0 tarball signature:

```bash
gpg --verify flink-cdc-3.1.0-src.tgz.asc flink-cdc-3.1.0-src.tgz
```

It should print Good signature from "Leonard Xu ...".

3. Verify that the binary package was compiled with JDK 8.

Unpack the precompiled binary jar package and check if the `Build-Jdk` entry in the `META-INF\MANIFEST.MF` file is correct.

4. Run migration tests.

Flink CDC tries to ensure backward compatibility of state, that is, the job state (Checkpoint/Savepoint) saved with previous CDC version should be usable in the new version.
You can run CDC migration verification locally with [Flink CDC Migration Test Utils](https://github.com/yuxiqian/migration-test) script.

* [Pipeline Job Migration Test Guide](https://github.com/yuxiqian/migration-test/blob/main/README.md)
* [DataStream Job Migration Test Guide](https://github.com/yuxiqian/migration-test/blob/main/datastream/README.md)

5. Run end-to-end tests.

You may configure the RC version of Flink CDC together with a supported Flink version,
try running some Pipeline/SQL/DataStream jobs,
and observe if jobs are working properly.

6. Check third-party software licenses.

Flink CDC depends on a few third-party open source software, 
we need to ensure their licenses are correctly included in the NOTICE file.

In addition, some license agreements are incompatible with the Apache 2.0 license used by Flink CDC,
and any software licensed under these agreements cannot be packaged into Flink CDC.
You can get more information at [ASF 3RD PARTY LICENSE POLICY](https://www.apache.org/legal/resolved.html).
