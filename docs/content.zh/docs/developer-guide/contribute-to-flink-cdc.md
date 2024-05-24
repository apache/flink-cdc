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

| 贡献方式  | 更多信息                                                                                                                                                                            |
|:------|:-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 提交BUG | 为了提交问题，您需要首先在 [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) 建立对应的issue，并在`Component/s`选择`Flink CDC`。然后在问题描述中详细描述遇到的问题的信息，如果可能的话，最好提供一下能够复现问题的操作步骤。         |
| 贡献代码  | 请阅读 <a href="#code-contribution-guide">贡献代码指导</a>                                                                                                                                    |
| 代码评审  | 请阅读 <a href="#code-review-guide">代码评审指导</a>                                                                                                                                          |
| 用户支持  | 通过 [Flink 用户邮件列表](https://flink.apache.org/what-is-flink/community/#mailing-lists) 来帮助回复用户问题，在 [Flink jira](https://issues.apache.org/jira/projects/FLINK/issues) 可以查询到最新的已知问题。 |

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
