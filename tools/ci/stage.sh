#!/usr/bin/env bash
################################################################################
#  Copyright 2023 Ververica Inc.
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
STAGE_CORE="core"
STAGE_PIPELINE_CONNECTORS="pipeline_connectors"
STAGE_MYSQL="mysql"
STAGE_POSTGRES="postgres"
STAGE_ORACLE="oracle"
STAGE_MONGODB="mongodb"
STAGE_SQLSERVER="sqlserver"
STAGE_TIDB="tidb"
STAGE_OCEANBASE="oceanbase"
STAGE_DB2="db2"
STAGE_VITESS="vitess"
STAGE_E2E="e2e"
STAGE_MISC="misc"

MODULES_CORE="\
flink-cdc-cli,\
flink-cdc-common,\
flink-cdc-composer,\
flink-cdc-runtime"

MODULES_PIPELINE_CONNECTORS="\
flink-cdc-connect/flink-cdc-pipeline-connectors"

MODULES_MYSQL="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mysql-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mysql-cdc"

MODULES_POSTGRES="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-postgres-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-postgres-cdc"

MODULES_ORACLE="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oracle-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oracle-cdc"

MODULES_MONGODB="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-mongodb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-mongodb-cdc"

MODULES_SQLSERVER="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-sqlserver-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-sqlserver-cdc"

MODULES_TIDB="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-tidb-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-tidb-cdc"

MODULES_OCEANBASE="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-oceanbase-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-oceanbase-cdc"

MODULES_DB2="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-db2-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-db2-cdc"

MODULES_VITESS="\
flink-cdc-connect/flink-cdc-source-connectors/flink-connector-vitess-cdc,\
flink-cdc-connect/flink-cdc-source-connectors/flink-sql-connector-vitess-cdc"

MODULES_E2E="\
flink-cdc-e2e-tests/flink-cdc-pipeline-e2e-tests,\
flink-cdc-e2e-tests/flink-cdc-source-e2e-tests"

function get_compile_modules_for_stage() {
    local stage=$1

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $MODULES_CORE -am"
        ;;
        (${STAGE_PIPELINE_CONNECTORS})
            echo "-pl $MODULES_PIPELINE_CONNECTORS -am"
        ;;
        (${STAGE_MYSQL})
            echo "-pl $MODULES_MYSQL -am"
        ;;
        (${STAGE_POSTGRES})
            echo "-pl $MODULES_POSTGRES -am"
        ;;
        (${STAGE_ORACLE})
            echo "-pl $MODULES_ORACLE -am"
        ;;
        (${STAGE_MONGODB})
            echo "-pl $MODULES_MONGODB -am"
        ;;
        (${STAGE_SQLSERVER})
            echo "-pl $MODULES_SQLSERVER -am"
        ;;
        (${STAGE_TIDB})
            echo "-pl $MODULES_TIDB -am"
        ;;
        (${STAGE_OCEANBASE})
            echo "-pl $MODULES_OCEANBASE -am"
        ;;
        (${STAGE_DB2})
            echo "-pl $MODULES_DB2 -am"
        ;;
        (${STAGE_VITESS})
            echo "-pl $MODULES_VITESS -am"
        ;;
        (${STAGE_E2E})
            # compile everything; using the -am switch does not work with negated module lists!
            # the negation takes precedence, thus not all required modules would be built
            echo ""
        ;;
        (${STAGE_MISC})
            # compile everything; using the -am switch does not work with negated module lists!
            # the negation takes precedence, thus not all required modules would be built
            echo ""
        ;;
    esac
}

function get_test_modules_for_stage() {
    local stage=$1

    local modules_core=$MODULES_CORE
    local modules_pipeline_connectors=$MODULES_PIPELINE_CONNECTORS
    local modules_mysql=$MODULES_MYSQL
    local modules_postgres=$MODULES_POSTGRES
    local modules_oracle=$MODULES_ORACLE
    local modules_mongodb=$MODULES_MONGODB
    local modules_sqlserver=$MODULES_SQLSERVER
    local modules_tidb=$MODULES_TIDB
    local modules_oceanbase=$MODULES_OCEANBASE
    local modules_db2=$MODULES_DB2
    local modules_vitess=$MODULES_VITESS
    local modules_e2e=$MODULES_E2E
    local negated_mysql=\!${MODULES_MYSQL//,/,\!}
    local negated_postgres=\!${MODULES_POSTGRES//,/,\!}
    local negated_oracle=\!${MODULES_ORACLE//,/,\!}
    local negated_mongodb=\!${MODULES_MONGODB//,/,\!}
    local negated_sqlserver=\!${MODULES_SQLSERVER//,/,\!}
    local negated_tidb=\!${MODULES_TIDB//,/,\!}
    local negated_oceanbase=\!${MODULES_OCEANBASE//,/,\!}
    local negated_db2=\!${MODULES_DB2//,/,\!}
    local negated_vitess=\!${MODULES_vitess//,/,\!}
    local negated_e2e=\!${MODULES_E2E//,/,\!}
    local modules_misc="$negated_mysql,$negated_postgres,$negated_oracle,$negated_mongodb,$negated_sqlserver,$negated_tidb,$negated_oceanbase,$negated_db2,$negated_vitess,$negated_e2e"

    case ${stage} in
        (${STAGE_CORE})
            echo "-pl $modules_core"
        ;;
        (${STAGE_PIPELINE_CONNECTORS})
            echo "-pl $modules_pipeline_connectors"
        ;;
        (${STAGE_MYSQL})
            echo "-pl $modules_mysql"
        ;;
        (${STAGE_POSTGRES})
            echo "-pl $modules_postgres"
        ;;
        (${STAGE_ORACLE})
            echo "-pl $modules_oracle"
        ;;
        (${STAGE_MONGODB})
            echo "-pl $modules_mongodb"
        ;;
        (${STAGE_SQLSERVER})
            echo "-pl $modules_sqlserver"
        ;;
        (${STAGE_TIDB})
            echo "-pl $modules_tidb"
        ;;
        (${STAGE_OCEANBASE})
            echo "-pl $modules_oceanbase"
        ;;
        (${STAGE_DB2})
            echo "-pl $modules_db2"
        ;;
        (${STAGE_VITESS})
            echo "-pl $modules_vitess"
        ;;
        (${STAGE_E2E})
            echo "-pl $modules_e2e"
        ;;
        (${STAGE_MISC})
            echo "-pl $modules_misc"
        ;;
    esac
}
