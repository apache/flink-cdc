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

const metadata = require('./site-metadata');
const baseUrl = process.env.DOCS_BASE_URL || '/flink/flink-cdc-docs-master/';

/** @type {import('@docusaurus/types').Config} */
module.exports = {
  title: 'Apache Flink CDC',
  url: 'https://nightlies.apache.org',
  baseUrl,
  trailingSlash: true,
  favicon: 'favicon.png',
  onBrokenLinks: 'throw',
  onBrokenAnchors: 'throw',
  markdown: { format: 'detect' },
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'zh'],
    localeConfigs: {
      en: { label: 'English' },
      zh: { label: '中文版', htmlLang: 'zh-CN' },
    },
  },
  customFields: { ...metadata, rootBaseUrl: baseUrl },
  clientModules: [require.resolve('./src/client/matomo')],
  presets: [
    [
      'classic',
      {
        docs: {
          path: 'content',
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
          breadcrumbs: false,
          editUrl: ({ locale: language, docPath }) => {
            const directory =
              language === 'en'
                ? 'content'
                : `i18n/${language}/docusaurus-plugin-content-docs/current`;
            return `${metadata.repository}/edit/${metadata.branch}/docs/${directory}/${docPath}`;
          },
          beforeDefaultRemarkPlugins: [
            [
              require('./src/plugins/remark-variables'),
              { ...metadata, baseUrl },
            ],
          ],
        },
        blog: false,
        pages: false,
        theme: { customCss: ['./src/css/custom.css', './src/css/home.css'] },
      },
    ],
  ],
  themes: [
    [
      '@easyops-cn/docusaurus-search-local',
      {
        hashed: true,
        language: ['en', 'zh'],
        indexBlog: false,
        indexPages: false,
        docsDir: 'content',
        docsRouteBasePath: '/',
      },
    ],
  ],
  plugins: [
    [
      '@docusaurus/plugin-client-redirects',
      {
        redirects: require('./redirects'),
      },
    ],
  ],
  themeConfig: {
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    navbar: {
      items: [
        { type: 'docsVersionDropdown', position: 'right' },
        { type: 'localeDropdown', position: 'right' },
        {
          href: metadata.repository,
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    docs: { sidebar: { autoCollapseCategories: false } },
    prism: {
      theme: require('prism-react-renderer').themes.github,
      additionalLanguages: [
        'java',
        'scala',
        'sql',
        'bash',
        'json',
        'yaml',
        'markup',
        'properties',
        'docker',
      ],
    },
  },
};
