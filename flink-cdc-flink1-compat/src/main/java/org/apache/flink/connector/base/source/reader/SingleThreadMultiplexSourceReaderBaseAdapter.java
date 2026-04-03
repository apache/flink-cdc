package org.apache.flink.connector.base.source.reader;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;

import javax.annotation.Nullable;

public abstract class SingleThreadMultiplexSourceReaderBaseAdapter<
                E, T, SplitT extends SourceSplit, SplitStateT>
        extends SingleThreadMultiplexSourceReaderBase<E, T, SplitT, SplitStateT> {

    public SingleThreadMultiplexSourceReaderBaseAdapter(
            SingleThreadFetcherManager<E, SplitT> splitFetcherManager,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            @Nullable RecordEvaluator<T> eofRecordEvaluator,
            Configuration config,
            SourceReaderContext context,
            RateLimiterStrategy rateLimiterStrategy) {
        super(splitFetcherManager, recordEmitter, eofRecordEvaluator, config, context);
    }
}
