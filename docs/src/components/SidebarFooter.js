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

import React from 'react';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';

export default function SidebarFooter() {
  const { siteConfig, i18n } = useDocusaurusContext();
  const { rootBaseUrl, previousDocs, versionTitle } = siteConfig.customFields;
  const chinese = i18n.currentLocale === 'zh';
  return (
    <footer className="cdc-sidebar-footer">
      <a href="https://flink.apache.org/">↗ Project Homepage</a>
      <a href={`${rootBaseUrl}api/java/`}>↗ JavaDocs</a>
      <details className="cdc-version-picker">
        <summary>{chinese ? '选择文档版本' : 'Pick Docs Version'}</summary>
        <Link to="/">{versionTitle} ✓</Link>
        {previousDocs.slice(0, 2).map(({ label, href }) => (
          <a key={label} href={href + (chinese ? 'zh/' : '')}>
            v{label}
          </a>
        ))}
        <Link to="/versions/">{chinese ? '所有版本' : 'All Versions'}</Link>
      </details>
    </footer>
  );
}
