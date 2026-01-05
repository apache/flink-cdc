#!/usr/bin/env python3

import sys

if len(sys.argv) != 3:
    raise RuntimeError(f"Usage: {sys.argv[0]} [compile | test] [modules to test, concatenated with `,`]")

MODE = sys.argv[1]
INPUT_MODULES = sys.argv[2]

MODULES_CORE = [
    "flink-cdc-cli",
    "flink-cdc-common",
    "flink-cdc-composer",
    "flink-cdc-runtime",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-cdc-base",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-values"
]

MODULES_PIPELINE_CONNECTORS = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-elasticsearch",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-iceberg",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-kafka",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-maxcompute",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-oceanbase",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-maxcompute",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-starrocks",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-fluss",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-hudi",
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-values"
]

MODULES_MYSQL_SOURCE = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc"
]

MODULES_MYSQL_PIPELINE = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-mysql"
]

MODULES_POSTGRES_SOURCE = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"
]

MODULES_POSTGRES_PIPELINE = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-postgres"
]

MODULES_ORACLE = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc"
]

MODULES_MONGODB = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mongodb-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc"
]

MODULES_SQLSERVER = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-sqlserver-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc"
]

MODULES_TIDB = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-tidb-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-tidb-cdc"
]

MODULES_OCEANBASE_SOURCE = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oceanbase-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oceanbase-cdc"
]

MODULES_OCEANBASE_PIPELINE = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-oceanbase"
]

MODULES_DB2 = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-db2-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-db2-cdc"
]

MODULES_VITESS = [
    "flink-cdc-connect/flink-cdc-source-connectors/flink-connector-vitess-cdc",
    "flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-vitess-cdc"
]

MODULES_DORIS = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-doris"
]

MODULES_STARROCKS = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-starrocks"
]

MODULES_ICEBERG = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-iceberg"
]

MODULES_KAFKA = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-kafka"
]

MODULES_PAIMON = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-paimon"
]

MODULES_ELASTICSEARCH = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-elasticsearch"
]

MODULES_MAXCOMPUTE = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-maxcompute"
]

MODULES_FLUSS = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-fluss"
]

MODULES_HUDI = [
    "flink-cdc-connect/flink-cdc-pipeline-connectors/flink-cdc-pipeline-connector-hudi"
]

MODULES_PIPELINE_E2E = [
    "flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests"
]

MODULES_SOURCE_E2E = [
    "flink-cdc-e2e-tests/flink-cdc-source-e2e-tests"
]

ALL_MODULES = set(
    MODULES_CORE +
    MODULES_PIPELINE_CONNECTORS +
    MODULES_MYSQL_SOURCE +
    MODULES_MYSQL_PIPELINE +
    MODULES_POSTGRES_SOURCE +
    MODULES_POSTGRES_PIPELINE +
    MODULES_ORACLE +
    MODULES_MONGODB +
    MODULES_SQLSERVER +
    MODULES_TIDB +
    MODULES_OCEANBASE_SOURCE +
    MODULES_OCEANBASE_PIPELINE +
    MODULES_DB2 +
    MODULES_VITESS +
    MODULES_DORIS +
    MODULES_STARROCKS +
    MODULES_ICEBERG +
    MODULES_KAFKA +
    MODULES_PAIMON +
    MODULES_ELASTICSEARCH +
    MODULES_MAXCOMPUTE +
    MODULES_FLUSS +
    MODULES_HUDI +
    MODULES_PIPELINE_E2E +
    MODULES_SOURCE_E2E
)

test_modules = set()
compile_modules = set()

for module in INPUT_MODULES.split(', '):
    module_list = set(globals()['MODULES_' + module.upper().replace('-', '_')])
    test_modules |= module_list
    if module == 'source_e2e' or module == 'pipeline_e2e':
        compile_modules |= ALL_MODULES
    else:
        compile_modules |= module_list

if MODE == 'compile':
    print(','.join(compile_modules))
elif MODE == 'test':
    print(','.join(test_modules))
else:
    raise RuntimeError(f"Unexpected mode: {MODE}")
