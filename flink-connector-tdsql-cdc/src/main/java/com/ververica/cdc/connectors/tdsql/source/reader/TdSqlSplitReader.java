package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * The {@link SplitReader} implementation for the {@link
 * com.ververica.cdc.connectors.tdsql.source.TdSqlSource}.
 */
public class TdSqlSplitReader implements SplitReader<SourceRecord, TdSqlSplit> {

    private final MySqlSplitReader mySqlSplitReader;

    public TdSqlSplitReader(MySqlSplitReader mySqlSplitReader) {
        this.mySqlSplitReader = mySqlSplitReader;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        return mySqlSplitReader.fetch();
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TdSqlSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }
        mySqlSplitReader.handleSplitsChanges(asMySqlSplit(splitsChanges));
    }

    private SplitsChange<MySqlSplit> asMySqlSplit(SplitsChange<TdSqlSplit> splitsChanges) {
        List<MySqlSplit> mySqlSplits =
                splitsChanges.splits().stream()
                        .map(TdSqlSplit::mySqlSplit)
                        .collect(Collectors.toList());
        return new SplitsAddition<>(mySqlSplits);
    }

    @Override
    public void wakeUp() {}

    @Override
    public void close() throws Exception {
        mySqlSplitReader.close();
    }
}
