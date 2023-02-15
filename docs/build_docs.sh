#!/bin/bash
################################################################################
#  Copyright 2022 Ververica Inc.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -x

# step-1: install dependencies
apt-get update
apt-get -y install git rsync python3-pip python3-git python3-stemmer python3-virtualenv python3-setuptools
python3 -m pip install -U sphinx==4.1.1 myst-parser==0.15.2 pygments==2.10.0 sphinx-rtd-theme==0.5.2

export REPO_NAME="${GITHUB_REPOSITORY##*/}"

git config --global --add safe.directory /__w/${REPO_NAME}/${REPO_NAME}
export SOURCE_DATE_EPOCH=$(git log -1 --pretty=%ct)
temp_docs_root=`mktemp -d`

ls

# step-1.5: copy main site content to temp dir
# this must be done before `make -C docs clean` otherwise the contents will be removed
rsync -avz "docs/site/" "${temp_docs_root}/"

# step-2: build sites for all branches(for multiple versioned docs), excludes 'HEAD' and 'gh-pages'
make -C docs clean
branches="`git for-each-ref '--format=%(refname:lstrip=-1)' refs/remotes/origin/ | grep -viE '^(HEAD|gh-pages|release-1.0|release-1.1|release-1.2|release-1.3)$'| grep -iE '^(release-|master)'`"
for current_branch in ${branches}; do
   export current_version=${current_branch}
   git checkout ${current_branch}

   # skip the branch that has no docs
   if [ ! -e 'docs/conf.py' ]; then
      echo -e "\tINFO: Couldn't find 'docs/conf.py' for branch: ${current_branch}, just skip this branch"
      continue
   fi
   echo "INFO: Building sites for branch: ${current_branch}"
   sphinx-build -b html docs/ docs/_build/html/${current_branch}

   # copy the build content to temp dir
   rsync -av "docs/_build/html/" "${temp_docs_root}/"

done

git checkout master
git config --global user.name "${GITHUB_ACTOR}"
git config --global user.email "${GITHUB_ACTOR}@users.noreply.github.com"

# step-3: push build sites to gh-pages branch
pushd "${temp_docs_root}"
git init
git remote add deploy "https://token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
git checkout -b gh-pages

touch .nojekyll

git add .
git commit -m "Generated docs from commit ${GITHUB_SHA}"
git push deploy gh-pages --force

# pop back and exit
popd
exit 0
