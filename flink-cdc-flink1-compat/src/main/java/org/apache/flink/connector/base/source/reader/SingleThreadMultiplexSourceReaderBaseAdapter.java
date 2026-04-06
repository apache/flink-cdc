package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public abstract class SingleThreadMultiplexSourceReaderBaseAdapter<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT> {

    private static final Logger LOG =
            LoggerFactory.getLogger(SingleThreadMultiplexSourceReaderBaseAdapter.class);

    public SingleThreadMultiplexSourceReaderBaseAdapter(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context,
            @Nullable RateLimiterStrategy rateLimiterStrategy) {
        super(splitFetcherManager, recordEmitter, eofRecordEvaluator, config, context);
        if (null != rateLimiterStrategy) {
            LOG.warn(
                    "Because the runtime environment is Flink 1.x, the connector options `records.per.second` is ignored.");
        }
    }
}
