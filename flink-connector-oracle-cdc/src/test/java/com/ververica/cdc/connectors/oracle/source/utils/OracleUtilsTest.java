package com.ververica.cdc.connectors.oracle.source.utils;

import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import io.debezium.relational.TableId;
import org.junit.Test;

import java.util.Arrays;

/** Integration tests for Oracle Utils. */
public class OracleUtilsTest {

    @Test
    public void buildSplitScanQuery() {
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        true,
                        false));
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        false,
                        false));
        System.out.println(
                OracleUtils.buildSplitScanQuery(
                        TableId.parse("DEBEZIUM.PRODUCTS"),
                        new RowType(
                                Arrays.asList(new RowType.RowField("ROWID", new VarCharType()))),
                        false,
                        true));
    }
}
