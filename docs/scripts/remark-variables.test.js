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

const { test } = require('node:test');
const assert = require('node:assert/strict');
const remarkVariables = require('../src/plugins/remark-variables');

async function compile(source, locale) {
  const { compile: compileMarkdown } = await import('@mdx-js/mdx');
  return String(
    await compileMarkdown(source, {
      format: 'md',
      remarkPlugins: [
        [
          remarkVariables,
          {
            version: '3.8-SNAPSHOT',
            baseUrl: '/flink/flink-cdc-docs-master/',
            locale,
          },
        ],
      ],
    }),
  );
}

test('versions in shell commands, inline code, and download URLs are expanded', async () => {
  const output = await compile(
    [
      'Version @@VERSION@@; `flink-cdc-@@VERSION@@.jar`',
      '',
      '```bash',
      'docker build -t flink-cdc:@@VERSION@@ .',
      'echo {{REST_PORT}}',
      '```',
      '',
      '[Download](https://example.com/@@VERSION@@/flink-cdc-@@VERSION@@.jar)',
    ].join('\n'),
  );
  assert.ok(output.includes('flink-cdc-3.8-SNAPSHOT.jar'));
  assert.ok(output.includes('docker build -t flink-cdc:3.8-SNAPSHOT .'));
  assert.ok(
    output.includes(
      'https://example.com/3.8-SNAPSHOT/flink-cdc-3.8-SNAPSHOT.jar',
    ),
  );
  assert.ok(output.includes('{{REST_PORT}}'));
  assert.ok(!output.includes('@@VERSION@@'));
});

test('localized links retain the deployment prefix and Chinese route', async () => {
  const source =
    '[Guide](@@BASE_URL@@@@LOCALE_PREFIX@@docs/get-started/introduction/)';
  const english = await compile(source);
  const defaultDevelopment = await compile(source, 'undefined');
  const chinese = await compile(source, 'zh');
  assert.ok(
    english.includes(
      '/flink/flink-cdc-docs-master/docs/get-started/introduction/',
    ),
  );
  assert.ok(
    chinese.includes(
      '/flink/flink-cdc-docs-master/zh/docs/get-started/introduction/',
    ),
  );
  assert.ok(!chinese.includes('@@'));
  assert.equal(defaultDevelopment, english);
});

test('the locale is read when the compiler runs, after the site config was loaded', async () => {
  const previous = process.env.DOCUSAURUS_CURRENT_LOCALE;
  try {
    process.env.DOCUSAURUS_CURRENT_LOCALE = 'zh';
    const output = await compile('[Guide](@@BASE_URL@@@@LOCALE_PREFIX@@docs/get-started/introduction/)');
    assert.ok(output.includes('/flink/flink-cdc-docs-master/zh/docs/get-started/introduction/'));
  } finally {
    if (previous === undefined) delete process.env.DOCUSAURUS_CURRENT_LOCALE;
    else process.env.DOCUSAURUS_CURRENT_LOCALE = previous;
  }
});
