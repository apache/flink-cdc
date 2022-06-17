package com.ververica.cdc.connectors.tdsql.source.reader;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import com.ververica.cdc.connectors.mysql.source.reader.MySqlSplitReader;
import com.ververica.cdc.connectors.mysql.source.split.MySqlSplit;
import com.ververica.cdc.connectors.tdsql.bases.set.TdSqlSet;
import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;
import com.ververica.cdc.connectors.tdsql.source.split.TdSqlRecords;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * The {@link SplitReader} implementation for the {@link
 * com.ververica.cdc.connectors.tdsql.source.TdSqlSource}.
 */
public class TdSqlSplitReader implements SplitReader<SourceRecord, TdSqlSplit> {
    private static final Logger LOGGER = LoggerFactory.getLogger(TdSqlSourceReader.class);
    private MySqlSplitReader mySqlSplitReader;
    private final Function<TdSqlSet, MySqlSplitReader> getRealReader;

    private TdSqlSet tdSqlSet;

    public TdSqlSplitReader(Function<TdSqlSet, MySqlSplitReader> getRealReader) {
        this.getRealReader = getRealReader;
    }

    @Override
    public RecordsWithSplitIds<SourceRecord> fetch() throws IOException {
        RecordsWithSplitIds<SourceRecord> data = mySqlSplitReader.fetch();
        return new TdSqlRecords(data, tdSqlSet);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<TdSqlSplit> splitsChanges) {
        if (!(splitsChanges instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitsChanges.getClass()));
        }

        if (mySqlSplitReader == null) {
            TdSqlSplit tdSqlSplit = splitsChanges.splits().get(0);
            LOGGER.trace("init mySqlSplitReader.");
            mySqlSplitReader = getRealReader.apply(tdSqlSplit.setInfo());
            this.tdSqlSet = tdSqlSplit.setInfo();
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
