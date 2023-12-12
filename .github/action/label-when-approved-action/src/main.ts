import * as github from '@actions/github'
import * as core from '@actions/core'
import * as rest from '@octokit/rest'

function getRequiredEnv(key: string): string {
  const value = process.env[key]
  if (value === undefined) {
    const message = `${key} was not defined.`
    throw new Error(message)
  }
  return value
}

function verboseOutput(name: string, value: string): void {
  core.info(`Setting output: ${name}: ${value}`)
  core.setOutput(name, value)
}

async function getPullRequest(
  octokit: github.GitHub,
  owner: string,
  repo: string,
  pullRequestNumber: number
): Promise<rest.PullsGetResponse> {
  core.info(`Fetching pull request with number: ${pullRequestNumber}`)
  const pullRequest = await octokit.pulls.get({
    owner,
    repo,
    // eslint-disable-next-line @typescript-eslint/camelcase
    pull_number: pullRequestNumber
  })
  return pullRequest.data
}

function getPullRequestLabels(pullRequest: rest.PullsGetResponse): string[] {
  return pullRequest ? pullRequest.labels.map(label => label.name) : []
}

async function getReviews(
  octokit: github.GitHub,
  owner: string,
  repo: string,
  number: number,
  getComitters: boolean
): Promise<[rest.PullsListReviewsResponseItem[], string[], string[]]> {
  let reviews: rest.PullsListReviewsResponseItem[] = []
  const options = octokit.pulls.listReviews.endpoint.merge({
    owner,
    repo,
    // eslint-disable-next-line @typescript-eslint/camelcase
    pull_number: number
  })
  await octokit.paginate(options).then(r => {
    reviews = r
  })

  const reviewers = reviews ? reviews.map(review => review.user.login) : []
  const reviewersAlreadyChecked: string[] = []
  const committers: string[] = []
  if (getComitters) {
    core.info('Checking reviewers permissions:')
    for (const reviewer of reviewers) {
      if (!reviewersAlreadyChecked.includes(reviewer)) {
        const p = await octokit.repos.getCollaboratorPermissionLevel({
          owner,
          repo,
          username: reviewer
        })
        const permission = p.data.permission
        if (permission === 'admin' || permission === 'write') {
          committers.push(reviewer)
        }
        core.info(`\t${reviewer}: ${permission}`)
        reviewersAlreadyChecked.push(reviewer)
      }
    }
  }
  return [reviews, reviewers, committers]
}

function processReviews(
  reviews: rest.PullsListReviewsResponseItem[],
  reviewers: string[],
  committers: string[],
  requireCommittersApproval: boolean
): boolean {
  let isApproved = false
  const reviewStates: {[user: string]: string} = {}

  for (const review of reviews) {
    if (review.state === 'APPROVED' || review.state === 'CHANGES_REQUESTED') {
      if (requireCommittersApproval && committers.includes(review.user.login)) {
        reviewStates[review.user.login] = review.state
      } else if (!requireCommittersApproval) {
        reviewStates[review.user.login] = review.state
      }
    }
  }

  core.info(`Reviews:`)
  for (const user in reviewStates) {
    core.info(`\t${user}: ${reviewStates[user].toLowerCase()}`)
  }

  for (const user in reviewStates) {
    if (reviewStates[user] === 'APPROVED') {
      isApproved = true
      break
    }
  }
  for (const user in reviewStates) {
    if (reviewStates[user] === 'CHANGES_REQUESTED') {
      isApproved = false
      break
    }
  }

  return isApproved
}

async function setLabel(
  octokit: github.GitHub,
  owner: string,
  repo: string,
  pullRequestNumber: number,
  label: string
): Promise<void> {
  core.info(`Setting label: ${label}`)
  await octokit.issues.addLabels({
    // eslint-disable-next-line @typescript-eslint/camelcase
    issue_number: pullRequestNumber,
    labels: [label],
    owner,
    repo
  })
}

async function removeLabel(
  octokit: github.GitHub,
  owner: string,
  repo: string,
  pullRequestNumber: number,
  label: string
): Promise<void> {
  core.info(`Removing label: ${label}`)
  await octokit.issues.removeLabel({
    // eslint-disable-next-line @typescript-eslint/camelcase
    issue_number: pullRequestNumber,
    name: label,
    owner,
    repo
  })
}

async function addComment(
  octokit: github.GitHub,
  owner: string,
  repo: string,
  pullRequestNumber: number,
  comment: string
): Promise<void> {
  core.info(`Adding comment: ${comment}`)
  await octokit.issues.createComment({
    owner,
    repo,
    // eslint-disable-next-line @typescript-eslint/camelcase
    issue_number: pullRequestNumber,
    body: comment
  })
}

// async function printDebug(
//   item: object | string | boolean | number,
//   description: string = ''
// ): Promise<void> {
//   const itemJson = JSON.stringify(item)
//   core.debug(`\n ######### ${description} ######### \n: ${itemJson}\n\n`)
// }

async function run(): Promise<void> {
  const token = core.getInput('token', {required: true})
  const userLabel = core.getInput('label') || 'not set'
  const requireCommittersApproval =
    core.getInput('require_committers_approval') === 'true'
  const removeLabelWhenApprovalMissing =
    core.getInput('remove_label_when_approval_missing') === 'true'
  const comment = core.getInput('comment') || ''
  const pullRequestNumberInput = core.getInput('pullRequestNumber') || 'not set'
  const octokit = new github.GitHub(token)
  const context = github.context
  const repository = getRequiredEnv('GITHUB_REPOSITORY')
  const eventName = getRequiredEnv('GITHUB_EVENT_NAME')
  const [owner, repo] = repository.split('/')
  let pullRequestNumber: number | undefined

  core.info(
    `\n############### Set Label When Approved start ##################\n` +
      `label: "${userLabel}"\n` +
      `requireCommittersApproval: ${requireCommittersApproval}\n` +
      `comment: ${comment}\n` +
      `pullRequestNumber: ${pullRequestNumberInput}`
  )

  if (eventName === 'pull_request_review') {
    pullRequestNumber = context.payload.pull_request
      ? context.payload.pull_request.number
      : undefined
    if (pullRequestNumber === undefined) {
      throw Error(`Could not find PR number in context payload.`)
    }
  } else if (eventName === 'workflow_run') {
    if (pullRequestNumberInput === 'not set') {
      core.warning(
        `If action is triggered by "workflow_run" then input "pullRequestNumber" is required.\n` +
          `It might be missing because the pull request might have been already merged or a fixup pushed to` +
          `the PR branch. None of the outputs will be set as we cannot find the right PR.`
      )
      return
    } else {
      pullRequestNumber = parseInt(pullRequestNumberInput)
    }
  } else {
    throw Error(
      `This action is only useful in "pull_request_review" or "workflow_run" triggered runs and you used it in "${eventName}"`
    )
  }

  // PULL REQUEST
  const pullRequest = await getPullRequest(
    octokit,
    owner,
    repo,
    pullRequestNumber
  )

  // LABELS
  const labelNames = getPullRequestLabels(pullRequest)

  // REVIEWS
  const [reviews, reviewers, committers] = await getReviews(
    octokit,
    owner,
    repo,
    pullRequest.number,
    requireCommittersApproval
  )
  const isApproved = processReviews(
    reviews,
    reviewers,
    committers,
    requireCommittersApproval
  )

  // HANDLE LABEL
  let isLabelShouldBeSet = false
  let isLabelShouldBeRemoved = false

  if (userLabel !== 'not set') {
    isLabelShouldBeSet = isApproved && !labelNames.includes(userLabel)
    isLabelShouldBeRemoved =
      !isApproved &&
      labelNames.includes(userLabel) &&
      removeLabelWhenApprovalMissing

    if (isLabelShouldBeSet) {
      await setLabel(octokit, owner, repo, pullRequest.number, userLabel)
      if (comment !== '') {
        await addComment(octokit, owner, repo, pullRequest.number, comment)
      }
    } else if (isLabelShouldBeRemoved) {
      await removeLabel(octokit, owner, repo, pullRequest.number, userLabel)
    }
  }

  // OUTPUT
  verboseOutput('isApproved', String(isApproved))
  verboseOutput('labelSet', String(isLabelShouldBeSet))
  verboseOutput('labelRemoved', String(isLabelShouldBeRemoved))
}

run()
  .then(() =>
    core.info(
      '\n############### Set Label When Approved complete ##################\n'
    )
  )
  .catch(e => core.setFailed(e.message))
