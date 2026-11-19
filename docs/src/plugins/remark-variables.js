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

module.exports = function remarkVariables({
  version,
  baseUrl,
  locale = process.env.DOCUSAURUS_CURRENT_LOCALE || 'en',
}) {
  const replacements = {
    '@@VERSION@@': version,
    '@@BASE_URL@@': baseUrl,
    '@@LOCALE_PREFIX@@': locale === 'zh' ? 'zh/' : '',
  };
  function visit(node) {
    for (const key of ['value', 'url']) {
      if (typeof node[key] === 'string') {
        for (const [token, value] of Object.entries(replacements)) {
          node[key] = node[key].split(token).join(value);
        }
      }
    }
    node.children?.forEach(visit);
  }
  return visit;
};
