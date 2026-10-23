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

module.exports = {
  docs: [
    {
      type: 'category',
      label: 'Get Started',
      collapsed: true,
      items: [
        'docs/get-started/introduction',
        {
          type: 'category',
          label: 'Quickstart for Flink 1.20',
          collapsed: true,
          items: [
            'docs/get-started/quickstart-for-1.20/mysql-to-doris',
            'docs/get-started/quickstart-for-1.20/mysql-to-starrocks',
            'docs/get-started/quickstart-for-1.20/cdc-up-quickstart-guide',
            'docs/get-started/quickstart-for-1.20/mysql-to-kafka',
            'docs/get-started/quickstart-for-1.20/postgres-to-fluss',
          ],
        },
        {
          type: 'category',
          label: 'Quickstart for Flink 2.2',
          collapsed: true,
          items: [
            'docs/get-started/quickstart-for-2.2/mysql-to-doris',
            'docs/get-started/quickstart-for-2.2/mysql-to-starrocks',
            'docs/get-started/quickstart-for-2.2/mysql-to-kafka',
            'docs/get-started/quickstart-for-2.2/postgres-to-fluss',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Core Concept',
      collapsed: true,
      items: [
        'docs/core-concept/data-pipeline',
        'docs/core-concept/data-source',
        'docs/core-concept/data-sink',
        'docs/core-concept/table-id',
        'docs/core-concept/transform',
        'docs/core-concept/route',
        'docs/core-concept/schema-evolution',
        'docs/core-concept/type-mappings',
        'docs/core-concept/ai-model',
      ],
    },
    {
      type: 'category',
      label: 'Connectors',
      collapsed: true,
      items: [
        {
          type: 'category',
          label: 'Pipeline Connectors',
          collapsed: true,
          items: [
            'docs/connectors/pipeline-connectors/overview',
            'docs/connectors/pipeline-connectors/mysql',
            'docs/connectors/pipeline-connectors/oracle',
            'docs/connectors/pipeline-connectors/postgres',
            'docs/connectors/pipeline-connectors/sqlserver',
            'docs/connectors/pipeline-connectors/paimon',
            'docs/connectors/pipeline-connectors/fluss',
            'docs/connectors/pipeline-connectors/kafka',
            'docs/connectors/pipeline-connectors/doris',
            'docs/connectors/pipeline-connectors/starrocks',
            'docs/connectors/pipeline-connectors/elasticsearch',
            'docs/connectors/pipeline-connectors/maxcompute',
            'docs/connectors/pipeline-connectors/oceanbase',
            'docs/connectors/pipeline-connectors/iceberg',
            'docs/connectors/pipeline-connectors/hudi',
          ],
        },
        {
          type: 'category',
          label: 'Flink Sources',
          collapsed: true,
          items: [
            'docs/connectors/flink-sources/overview',
            'docs/connectors/flink-sources/mysql-cdc',
            'docs/connectors/flink-sources/oracle-cdc',
            'docs/connectors/flink-sources/sqlserver-cdc',
            'docs/connectors/flink-sources/postgres-cdc',
            'docs/connectors/flink-sources/mongodb-cdc',
            'docs/connectors/flink-sources/db2-cdc',
            'docs/connectors/flink-sources/tidb-cdc',
            'docs/connectors/flink-sources/oceanbase-cdc',
            'docs/connectors/flink-sources/vitess-cdc',
            'docs/connectors/flink-sources/datastream-api-package-guidance',
            {
              type: 'category',
              label: 'Tutorials',
              collapsed: true,
              items: [
                'docs/connectors/flink-sources/tutorials/mongodb-tutorial',
                'docs/connectors/flink-sources/tutorials/db2-tutorial',
                'docs/connectors/flink-sources/tutorials/oceanbase-tutorial',
                'docs/connectors/flink-sources/tutorials/oracle-tutorial',
                'docs/connectors/flink-sources/tutorials/polardbx-tutorial',
                'docs/connectors/flink-sources/tutorials/sqlserver-tutorial',
                'docs/connectors/flink-sources/tutorials/tidb-tutorial',
                'docs/connectors/flink-sources/tutorials/build-real-time-data-lake-tutorial',
                'docs/connectors/flink-sources/tutorials/build-streaming-etl-tutorial',
              ],
            },
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Deployment',
      collapsed: true,
      items: [
        'docs/deployment/standalone',
        'docs/deployment/yarn',
        'docs/deployment/kubernetes',
      ],
    },
    {
      type: 'category',
      label: 'Developer Guide',
      collapsed: true,
      items: [
        'docs/developer-guide/understand-flink-cdc-api',
        'docs/developer-guide/contribute-to-flink-cdc',
        'docs/developer-guide/licenses',
      ],
    },
    {
      type: 'category',
      label: 'FAQ',
      collapsed: true,
      items: ['docs/faq/faq'],
    },
  ],
};
