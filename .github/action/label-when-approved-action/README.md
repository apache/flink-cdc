# Label When Approved action

<p><a href="https://github.com/TobKed/label-when-approved-action/actions">
<img alt="label-when-approved-action status"
    src="https://github.com/TobKed/label-when-approved-action/workflows/Test%20the%20build/badge.svg"></a>


<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Context and motivation](#context-and-motivation)
- [Inputs and outputs](#inputs-and-outputs)
  - [Inputs](#inputs)
  - [Outputs](#outputs)
- [Examples](#examples)
    - [Workflow Run event](#workflow-run-event)
    - [Pull Request Review event](#pull-request-review-event)
- [Development environment](#development-environment)
- [License](#license)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Context and motivation

Label When Approved is an action that checks is Pull Request is approved and assign label to it.
Label is not set or removed when Pull Request has awaiting requested changes.

Setting label is optional that only output can be used in the workflow.

The required input `require_committers_approval` says is approval can be done by people with read access to the repo
or by anyone. It may be useful in repositories which requires committers approvals like [Apache Software Foundation](https://github.com/apache/)
projects. By default, when the PR does not have "approved" status or looses its aproval status the label is removed.
This behaviour can be disabled when `remove_label_when_approval_missing` is set to false.

It can be used in workflows triggered by "pull_request_review" or "workflow_run".
When used on "pull_request_review" any workflows triggered from pull request created from fork will fail
due to insufficient permissions. Because of this support for "workflow_run" was added.
It should be triggered by workflows "pull_request_review" and requires additional input `pullRequestNumber`.
Pull request number can be obtained by using [potiuk/get-workflow-origin)](https://github.com/potiuk/get-workflow-origin) action (see example [workflow-run-event](#workflow-run-event)).


# Inputs and outputs

## Inputs

| Input                                | Required | Example                                                           | Comment                                                                       |
|--------------------------------------|----------|-------------------------------------------------------------------|-------------------------------------------------------------------------------|
| `token`                              | yes      | `${{ secrets.GITHUB_TOKEN }}`                                     | The github token passed from `${{ secrets.GITHUB_TOKEN }}`                    |
| `label`                              | no       | `Approved by committers`                                          | Label to be added/removed to the Pull Request if approved/not approved        |
| `require_committers_approval`        | no       | `true`                                                            | Is approval from user with write permission required                          |
| `remove_label_when_approval_missing` | no       | `true`                                                            | Should label be removed when approval is missing                              |
| `comment`                            | no       | `PR approved by at least one committer and no changes requested.` | Add optional comment to the PR when approved (requires label input to be set) |
| `pullRequestNumber`                  | no       | `${{ steps.source-run-info.outputs.pullRequestNumber }}`          | Pull request number if triggered  by "worfklow_run"                           |

## Outputs

| Output         |                              |
|----------------|------------------------------|
| `isApproved`   | is Pull Request approved     |
| `labelSet`     | was label set                |
| `labelRemoved` | was label removed            |

# Examples

### Workflow Run event

```yaml
name: "Label when approved"
on:
  workflow_run:
    workflows: ["Workflow triggered on pull_request_review"]
    types: ['requested']

jobs:

  label-when-approved:
    name: "Label when approved"
    runs-on: ubuntu-latest
    steps:
      - name: "Get information about the original trigger of the run"
        uses: potiuk/get-workflow-origin@v1_2
        id: source-run-info
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          sourceRunId: ${{ github.event.workflow_run.id }}
      - name: Label when approved by anyone
        uses: TobKed/label-when-approved-action@v1.2
        id: label-when-approved-by-anyone
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          require_committers_approval: 'true'
          label: 'Approved by committer'
          comment: 'PR approved by at least one committer and no changes requested.'
          pullRequestNumber: ${{ steps.source-run-info.outputs.pullRequestNumber }}
```

### Pull Request Review event

```yaml
name: Label when approved
on: pull_request_review

jobs:

  label-when-approved:
    name: "Label when approved"
    runs-on: ubuntu-latest
    outputs:
      isApprovedByCommiters: ${{ steps.label-when-approved-by-commiters.outputs.isApproved }}
      isApprovedByAnyone: ${{ steps.label-when-approved-by-anyone.outputs.isApproved }}
    steps:
      - name: Label when approved by commiters
        uses: TobKed/label-when-approved-action@v1.2
        id: label-when-approved-by-commiters
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          label: 'ready to merge (committers)'
          require_committers_approval: 'true'
          remove_label_when_approval_missing: 'false'
          comment: 'PR approved by at least one committer and no changes requested.'
      - name: Label when approved by anyone
        uses: TobKed/label-when-approved-action@v1.2
        id: label-when-approved-by-anyone
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
```

# Development environment

It is highly recommended tu use [pre commit](https://pre-commit.com). The pre-commits
installed via pre-commit tool handle automatically linting (including automated fixes) as well
as building and packaging Javascript index.js from the main.ts Typescript code, so you do not have
to run it yourself.

# License
[MIT License](LICENSE) covers the scripts and documentation in this project.
