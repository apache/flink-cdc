---
title: "向 Flink CDC 提交贡献"
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

# 社区贡献

Flink CDC 由开放和友好的社区开发而来，欢迎任何想要提供帮助的贡献者。有以下一些方式可以让贡献者和社区交流和做出贡献，包括提问，提交发现的
Bug报告，提议新的功能，加入社区邮件列表的讨论，贡献代码或文档，改进项目网站，发版前测试和编写Blog等。

## 你想要贡献什么？

Flink CDC 社区的贡献不仅限于为项目贡献代码，下面列举了一些可以在社区贡献的内容。

| 贡献方式      | 更多信息                                                                                                                                                                            |
|:----------|:--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 提交BUG     | 为了提交问题，您需要首先在 [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) 建立对应的issue，并在`Component/s`选择`Flink CDC`。然后在问题描述中详细描述遇到的问题的信息，如果可能的话，最好提供一下能够复现问题的操作步骤。         |
| 贡献代码      | 请阅读 <a href="#code-contribution-guide">贡献代码指导</a>                                                                                                                               |
| 代码评审      | 请阅读 <a href="#code-review-guide">代码评审指导</a>                                                                                                                                     |
| 版本验证      | 请阅读 <a href="#release-validation-guide">版本验证指导</a>                                                                                                                              |                                                            |
| 用户支持      | 通过 [Flink 用户邮件列表](https://flink.apache.org/what-is-flink/community/#mailing-lists) 来帮助回复用户问题，在 [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) 可以查询到最新的已知问题。 |
| 加入开发者邮件列表 | 关注 [Flink 开发者邮件列表](https://lists.apache.org/list.html?dev@flink.apache.org) 来参与设计方案讨论。                                                                                          |

如果还有其他问题，可以通过 Flink Dev 邮件列表寻求帮助。

<h2 id="code-contribution-guide">贡献代码指导</h2>

Flink CDC 项目通过众多贡献者的代码贡献来维护，改进和拓展，欢迎各种形式的社区贡献。

在 Flink CDC 社区可以自由的在任何时间提出自己的问题，通过社区 Dev 邮件列表进行交流或在任何感兴趣的 issue 下评论和讨论。

如果您想要为 Flink CDC 贡献代码，可以通过如下的方式。

1. 首先在 [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) 的想要负责的 issue
   下评论（最好在评论中解释下对于这个问题的理解，和后续的设计，如果可能的话也可以提供下 POC 的代码）。
2. 在这个 issue 被分配给你后，开始进行开发实现（提交信息请遵循`[FLINK-xxx][xxx] xxxxxxx`的格式）。
3. 开发完成后可以向 [Flink CDC](https://github.com/apache/flink-cdc) 项目提交 PR（请确保 Clone 的项目 committer 有操作权限）。
4. 找到一个开发者帮忙评审代码，评审前请确保 CI 通过。
5. Flink committer 确认代码贡献满足全部要求后，代码会被合并到代码仓库。

<h2 id="code-review-guide">代码评审指导</h2>

每一次的代码评审需要检查如下一些方面的内容。

1. 提交的 PR 是否被正确地描述了？

评审时，需要检查对应的 PR 是否合理的描述了本次修改的内容，能否支持评审人较快的理解和评审代码。对于比较琐碎的修改，不需要提供太过详细的信息。

2. 提交的 PR 代码质量是否符合标准？

- 代码是否遵循正确的软件开发习惯？
- 代码是否正确，鲁棒性如何，是否便于维护和拓展，是否是可测试的？
- 代码是否有可能影响到性能？、
- 代码修改是否已经被正确的测试？测试执行速度是否有问题？
- 项目依赖是否发生了变化，如果是，对应的 NOTICE 文件是否需要更新？
- 提交信息是否遵循`[FLINK-xxx][xxx] xxxxxxx`格式？

3. 文档是否需要更新？

如果代码提交加入了新的功能，这个新功能需要同时更新到文档中。

<h2 id="release-validation-guide">版本验证指导</h2>

我们会定期发布新版本的 Flink CDC。

根据 [Apache Software Foundation 版本发布规则](https://www.apache.org/legal/release-policy.html)，
我们会在每次发布前制作发布候选（Release Candidate, RC）版本，
并邀请社区成员对这一预发布版本进行测试与投票。

欢迎您在 `dev@flink.apache.org` 邮件列表中参与版本验证工作。
验证内容可能包括以下方面：

1. 验证源代码是否可以正常编译。

目前，Flink CDC 使用 [Maven](https://maven.apache.org/) 3 作为构建工具，并在 JDK 8 平台上进行编译。
您可以下载 RC 版本的源代码包，并使用 `mvn clean package -Dfast` 命令进行编译，
并留意任何意料之外的错误或警告。

2. 验证二进制包签名是否一致。

为了确保发布的预编译包没有被篡改，在发布任何二进制 tar 包时总会附上对应文件的哈希值，以便用户进行完整性校验。
您可以下载 RC 版本的二进制 tar 包，并使用以下命令计算其 SHA512 哈希值：

* Linux: `sha512sum flink-cdc-*-bin.tar.gz`
* macOS: `shasum -a 512 flink-cdc-*-bin.tar.gz`
* Windows (PowerShell): `Get-FileHash flink-cdc-*-bin.tar.gz -Algorithm SHA512 | Format-List`

并验证结果是否与发布页面上的哈希值一致。

为了验证软件包签名，您需要先安装 gpg（2.4 及更高版本）。

首先，从 [KEYS](https://dist.apache.org/repos/dist/release/flink/KEYS) 下载负责发布的项目 PMC / Committer 的 GPG 公钥并导入：

```bash
gpg --import <releaser-public-key>
```

例如：

```bash
gpg  --import leonard-xu.pub
```

这里，`leonard-xu.pub` 文件即为之前下载的 @leonardBang 的公钥。

然后，我们就可以使用 gpg 命令来验证签名：

```bash
gpg --verify flink-cdc-<VERSION>-src.tgz.asc flink-cdc-<VERSION>-src.tgz
```

例如，使用以下命令来验证 3.1.0 的源码包是否被正确签名：

```bash
gpg --verify flink-cdc-3.1.0-src.tgz.asc flink-cdc-3.1.0-src.tgz
```

正常情况下应该打印出 Good signature from "Leonard Xu ..."。

3. 验证二进制包是否使用 JDK 8 进行编译。

解压预编译的二进制 jar 包，并检查其中的 `META-INF\MANIFEST.MF` 文件的 `Build-Jdk` 条目是否正确。

4. 执行迁移测试。

Flink CDC 尽力确保状态向后兼容性，即使用旧版本 CDC 保存的作业状态（Checkpoint / Savepoint）能够用于新版本的作业恢复。
您可以通过 [Flink CDC Migration Test Utils](https://github.com/apache/flink-cdc/tree/master/tools/mig-test) 脚本执行 CDC 迁移验证。

* [Pipeline 作业迁移测试指南](https://github.com/apache/flink-cdc/blob/master/tools/mig-test/README.md)
* [DataStream 作业迁移测试指南](https://github.com/apache/flink-cdc/blob/master/tools/mig-test/datastream/README.md)

5. 执行端到端测试。

您可以使用 RC 版本的 Flink CDC 与受支持的 Flink 版本一起，
尝试编写一些 Pipeline / SQL / DataStream 作业，
观察作业工作状况是否正常。

6. 检查许可协议。

Flink CDC 依赖许多第三方开源软件，我们需要确保这些依赖的许可证信息被正确地包含在 NOTICE 文件中。

此外，一些许可协议与 Flink CDC 采用的 Apache 2.0 许可证不兼容，
使用这些协议的软件不能被打包到 Flink CDC 中。
您可以在 [ASF 3RD PARTY LICENSE POLICY](https://www.apache.org/legal/resolved.html) 获取更多信息。