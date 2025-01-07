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

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.common.SessionIdentifier;
import org.apache.flink.cdc.connectors.maxcompute.common.UncheckedOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeWriteOptions;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.tunnel.Configuration;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import com.aliyun.odps.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** common utils use for maxcompute connector. */
public class MaxComputeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeUtils.class);

    public static Odps getOdps(MaxComputeOptions maxComputeOptions) {
        Account account;
        if (StringUtils.isNullOrEmpty(maxComputeOptions.getStsToken())) {
            account =
                    new AliyunAccount(
                            maxComputeOptions.getAccessId(), maxComputeOptions.getAccessKey());
        } else {
            account =
                    new StsAccount(
                            maxComputeOptions.getAccessId(),
                            maxComputeOptions.getAccessKey(),
                            maxComputeOptions.getStsToken());
        }
        Odps odps = new Odps(account);
        odps.setEndpoint(maxComputeOptions.getEndpoint());
        odps.setTunnelEndpoint(maxComputeOptions.getTunnelEndpoint());
        odps.setDefaultProject(maxComputeOptions.getProject());
        odps.getRestClient().setReadTimeout(60);
        odps.getRestClient().setConnectTimeout(60);
        odps.setUserAgent("Flink CDC");
        return odps;
    }

    public static TableTunnel getTunnel(
            MaxComputeOptions maxComputeOptions, MaxComputeWriteOptions writeOptions) {
        Odps odps = getOdps(maxComputeOptions);
        Configuration configuration =
                Configuration.builder(odps)
                        .withRetryLogger(RetryUtils.getRetryLogger())
                        .withRetryPolicy(new RetryUtils.FlinkDefaultRetryPolicy())
                        .withCompressOptions(
                                MaxComputeUtils.compressOptionOf(
                                        writeOptions.getCompressAlgorithm()))
                        .withQuotaName(maxComputeOptions.getQuotaName())
                        .build();
        TableTunnel tunnel = new TableTunnel(odps, configuration);
        if (!StringUtils.isNullOrEmpty(maxComputeOptions.getTunnelEndpoint())) {
            tunnel.setEndpoint(maxComputeOptions.getTunnelEndpoint());
        }
        return tunnel;
    }

    public static Table getTable(MaxComputeOptions maxComputeOptions, TableId tableId) {
        Odps odps = getOdps(maxComputeOptions);
        if (maxComputeOptions.isSupportSchema()) {
            return odps.tables()
                    .get(
                            maxComputeOptions.getProject(),
                            tableId.getNamespace(),
                            tableId.getTableName());
        } else {
            return odps.tables().get(tableId.getTableName());
        }
    }

    public static TableSchema getTableSchema(MaxComputeOptions options, TableId tableId) {
        Odps odps = getOdps(options);
        if (options.isSupportSchema()) {
            return odps.tables()
                    .get(options.getProject(), tableId.getNamespace(), tableId.getTableName())
                    .getSchema();
        } else {
            return odps.tables().get(options.getProject(), tableId.getTableName()).getSchema();
        }
    }

    public static boolean supportSchema(MaxComputeOptions maxComputeOptions) {
        Odps odps = getOdps(maxComputeOptions);
        try {
            boolean flag =
                    Boolean.parseBoolean(
                            odps.projects().get().getProperty(Constant.SCHEMA_ENABLE_FLAG));
            LOG.info("project {} is support schema: {}", maxComputeOptions.getProject(), flag);
            return flag;
        } catch (OdpsException e) {
            throw new UncheckedOdpsException(e);
        }
    }

    public static CompressOption compressOptionOf(String compressAlgo) {
        CompressOption.CompressAlgorithm compressAlgorithm;
        switch (compressAlgo) {
            case "raw":
                compressAlgorithm = CompressOption.CompressAlgorithm.ODPS_RAW;
                break;
            case "zlib":
                compressAlgorithm = CompressOption.CompressAlgorithm.ODPS_ZLIB;
                break;
            case "lz4":
                compressAlgorithm = CompressOption.CompressAlgorithm.ODPS_LZ4_FRAME;
                break;
            case "snappy":
                compressAlgorithm = CompressOption.CompressAlgorithm.ODPS_SNAPPY;
                break;
            default:
                throw new IllegalArgumentException(
                        "unknown compress algo: "
                                + compressAlgo
                                + " , only support raw, zlib, lz4, snappy");
        }
        return new CompressOption(compressAlgorithm, 1, 0);
    }

    public static boolean isTableExist(MaxComputeOptions maxComputeOptions, TableId tableId) {
        Odps odps = getOdps(maxComputeOptions);
        try {
            if (maxComputeOptions.isSupportSchema()) {
                return odps.tables()
                        .exists(
                                odps.getDefaultProject(),
                                tableId.getNamespace(),
                                tableId.getTableName());
            } else {
                return odps.tables().exists(tableId.getTableName());
            }
        } catch (OdpsException e) {
            throw new UncheckedOdpsException(e);
        }
    }

    public static boolean schemaEquals(TableSchema currentSchema, TableSchema expectSchema) {
        List<Column> currentColumns = currentSchema.getAllColumns();
        List<Column> expectColumns = expectSchema.getAllColumns();
        if (currentColumns.size() != expectColumns.size()
                || currentSchema.getColumns().size() != expectSchema.getColumns().size()) {
            LOG.error(
                    "current column size not equals to expect column size: {}, {}",
                    currentColumns.size(),
                    expectColumns.size());
            return false;
        }
        for (int i = 0; i < currentColumns.size(); i++) {
            if (!currentColumns.get(i).getName().equalsIgnoreCase(expectColumns.get(i).getName())) {
                LOG.error(
                        "current column {} name not equals to expect column name: {}",
                        currentColumns.get(i).getName(),
                        expectColumns.get(i).getName());
                return false;
            }
            if (!currentColumns
                    .get(i)
                    .getTypeInfo()
                    .getTypeName()
                    .equals(expectColumns.get(i).getTypeInfo().getTypeName())) {
                LOG.error(
                        "current column {} type not equals to expect column type: {}",
                        currentColumns.get(i).getTypeInfo().getTypeName(),
                        expectColumns.get(i).getTypeInfo().getTypeName());
                return false;
            }
        }
        return true;
    }

    public static void createPartitionIfAbsent(
            MaxComputeOptions options, String schema, String table, String partitionName)
            throws OdpsException {
        Odps odps = getOdps(options);
        if (options.isSupportSchema()) {
            if (StringUtils.isNullOrEmpty(schema)) {
                LOG.info(
                        "create partition {} in {}.default.{}",
                        partitionName,
                        options.getProject(),
                        table);
                odps.tables()
                        .get(options.getProject(), "default", table)
                        .createPartition(new PartitionSpec(partitionName), true);
            } else {
                LOG.info(
                        "create partition {} in {}.{}.{}",
                        partitionName,
                        options.getProject(),
                        schema,
                        table);
                odps.tables()
                        .get(options.getProject(), schema, table)
                        .createPartition(new PartitionSpec(partitionName), true);
            }
        } else {
            LOG.info("create partition {} in {}.{}", partitionName, options.getProject(), table);
            odps.tables()
                    .get(options.getProject(), table)
                    .createPartition(new PartitionSpec(partitionName), true);
        }
    }

    public static String getSchema(MaxComputeOptions options, TableId tableId) {
        if (options.isSupportSchema()) {
            if (tableId.getNamespace() == null) {
                return "default";
            } else {
                return tableId.getNamespace();
            }
        } else {
            return null;
        }
    }

    public static boolean isTransactionalTable(
            MaxComputeOptions options, SessionIdentifier sessionIdentifier) {
        Odps odps = getOdps(options);
        return odps.tables()
                .get(
                        sessionIdentifier.getProject(),
                        sessionIdentifier.getSchema(),
                        sessionIdentifier.getTable())
                .isTransactional();
    }
}
