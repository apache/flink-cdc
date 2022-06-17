package com.ververica.cdc.connectors.tdsql.source.reader.fetcher;

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import com.ververica.cdc.connectors.tdsql.source.assigner.splitter.TdSqlSplit;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Fetcher manager for TDSQL.
 *
 * @param <T> The output type for flink.
 */
public class TdSqlFetcherManager<T> extends SplitFetcherManager<T, TdSqlSplit> {

    private final Map<String, Integer> splitFetcherMapping = new HashMap<>();
    private final Map<String, Boolean> fetcherStatus = new HashMap<>();

    /**
     * Creates a new SplitFetcherManager with multiple I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records). This must be the same queue instance
     *     that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderFactory The factory for the split reader that connects to the source
     */
    public TdSqlFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<T>> elementsQueue,
            Supplier<SplitReader<T, TdSqlSplit>> splitReaderFactory) {
        super(elementsQueue, splitReaderFactory);
    }

    @Override
    public void addSplits(List<TdSqlSplit> splitsToAdd) {
        for (TdSqlSplit split : splitsToAdd) {
            SplitFetcher<T, TdSqlSplit> fetcher = getOrCreateFetcher(split.setKey());
            fetcher.addSplits(Collections.singletonList(split));
            startFetcher(split.setKey(), fetcher);
        }
    }

    private void startFetcher(String setKey, SplitFetcher<T, TdSqlSplit> fetcher) {
        if (fetcherStatus.get(setKey) != Boolean.TRUE) {
            fetcherStatus.put(setKey, true);
            super.startFetcher(fetcher);
        }
    }

    private SplitFetcher<T, TdSqlSplit> getOrCreateFetcher(String setKey) {
        Integer fetcherId = splitFetcherMapping.get(setKey);

        SplitFetcher<T, TdSqlSplit> fetcher;
        if (fetcherId == null) {
            fetcher = createSplitFetcher();
            recordMapping(setKey);
        } else {
            fetcher = fetchers.get(fetcherId);

            if (fetcher == null) {
                fetcherStatus.remove(setKey);
                fetcher = createSplitFetcher();
                recordMapping(setKey);
            }
        }

        return fetcher;
    }

    private void recordMapping(String setKey) {
        splitFetcherMapping.put(setKey, getLatestFetcherId());
    }

    private Integer getLatestFetcherId() {
        int maxId = 0;

        for (Integer fid : fetchers.keySet()) {
            maxId = Math.max(maxId, fid);
        }
        return maxId;
    }
}
