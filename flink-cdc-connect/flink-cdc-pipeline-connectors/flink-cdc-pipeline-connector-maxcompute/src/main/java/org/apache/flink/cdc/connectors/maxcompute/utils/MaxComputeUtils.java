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
import org.apache.flink.cdc.common.utils.StringUtils;
import org.apache.flink.cdc.connectors.maxcompute.common.Constant;
import org.apache.flink.cdc.connectors.maxcompute.common.UncheckedOdpsException;
import org.apache.flink.cdc.connectors.maxcompute.options.MaxComputeOptions;

import com.aliyun.odps.Column;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.aliyun.odps.account.StsAccount;
import com.aliyun.odps.data.ArrayRecord;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.io.CompressOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/** common utils use for maxcompute connector. */
public class MaxComputeUtils {

    private static final Logger LOG = LoggerFactory.getLogger(MaxComputeUtils.class);

    public static Odps getOdps(MaxComputeOptions maxComputeOptions) {
        Account account;
        if (StringUtils.isNullOrWhitespaceOnly(maxComputeOptions.getStsToken())) {
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
        return odps;
    }

    public static TableTunnel getTunnel(MaxComputeOptions maxComputeOptions) {
        Odps odps = getOdps(maxComputeOptions);
        TableTunnel tunnel = new TableTunnel(odps);
        tunnel.getConfig().setQuotaName(maxComputeOptions.getQuotaName());
        return tunnel;
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
            default:
                compressAlgorithm = CompressOption.CompressAlgorithm.ODPS_SNAPPY;
        }
        return new CompressOption(compressAlgorithm, 1, 0);
    }

    public static String debugString(ArrayRecord arrayRecord) {
        if (arrayRecord == null) {
            return "NULL";
        }
        StringBuilder sb = new StringBuilder();
        for (Object o : arrayRecord.toArray()) {
            if (o == null) {
                sb.append("NULL").append(",");
            } else if (o instanceof byte[]) {
                sb.append(new String((byte[]) o)).append(o.getClass().getName()).append(",");
            } else {
                sb.append(o).append(o.getClass().getName()).append(",");
            }
        }
        sb.setLength(sb.length() - 1);
        return sb.toString();
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
            if (!currentColumns.get(i).getName().equals(expectColumns.get(i).getName())) {
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
            odps.tables()
                    .get(options.getProject(), schema, table)
                    .createPartition(new PartitionSpec(partitionName), true);
        } else {
            odps.tables()
                    .get(options.getProject(), table)
                    .createPartition(new PartitionSpec(partitionName), true);
        }
    }
}
