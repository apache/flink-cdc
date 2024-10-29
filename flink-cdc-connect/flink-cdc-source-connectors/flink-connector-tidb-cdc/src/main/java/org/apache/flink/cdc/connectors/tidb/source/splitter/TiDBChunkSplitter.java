package org.apache.flink.cdc.connectors.tidb.source.splitter;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.Column;
import io.debezium.relational.TableId;
import org.apache.flink.cdc.connectors.base.config.JdbcSourceConfig;
import org.apache.flink.cdc.connectors.base.dialect.JdbcDataSourceDialect;
import org.apache.flink.cdc.connectors.base.source.assigner.splitter.JdbcSourceChunkSplitter;
import org.apache.flink.table.types.DataType;

import java.sql.SQLException;

public class TiDBChunkSplitter extends JdbcSourceChunkSplitter {
  public TiDBChunkSplitter(JdbcSourceConfig sourceConfig, JdbcDataSourceDialect dialect) {
    super(sourceConfig, dialect);
  }

  @Override
  protected Object queryNextChunkMax(
      JdbcConnection jdbc,
      TableId tableId,
      Column splitColumn,
      int chunkSize,
      Object includedLowerBound)
      throws SQLException {
    return null;
  }

  @Override
  protected Long queryApproximateRowCnt(JdbcConnection jdbc, TableId tableId) throws SQLException {
    return null;
  }

  @Override
  protected DataType fromDbzColumn(Column splitColumn) {
    return null;
  }
}
