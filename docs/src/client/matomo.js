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

export function onRouteDidUpdate() {
  if (window.location.hostname !== 'nightlies.apache.org') return;
  const queue = (window._paq = window._paq || []);
  if (!document.getElementById('apache-matomo')) {
    queue.push(['disableCookies']);
    queue.push([
      'setDomains',
      ['*.flink.apache.org', '*.nightlies.apache.org/flink'],
    ]);
    queue.push(['setTrackerUrl', 'https://analytics.apache.org/matomo.php']);
    queue.push(['setSiteId', '1']);
    queue.push(['enableLinkTracking']);
    const script = document.createElement('script');
    script.id = 'apache-matomo';
    script.src = 'https://analytics.apache.org/matomo.js';
    script.async = true;
    document.head.appendChild(script);
  }
  queue.push(['setCustomUrl', window.location.href]);
  queue.push(['setDocumentTitle', document.title]);
  queue.push(['trackPageView']);
}
