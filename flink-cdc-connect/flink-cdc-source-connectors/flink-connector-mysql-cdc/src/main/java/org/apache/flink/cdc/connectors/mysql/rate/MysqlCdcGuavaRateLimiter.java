package org.apache.flink.cdc.connectors.mysql.rate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

/**
 * The SnapshotGuavaRateLimiter class implements Flink's RateLimiter interface, which is used to limit the read rate of data sources.
 * It uses Guava's RateLimiter to control the number of requests allowed to pass per second,
 * ensuring that parallel tasks can adjust the rate limit after the checkpoint is completed.
 */
public class MysqlCdcGuavaRateLimiter implements org.apache.flink.api.connector.source.util.ratelimit.RateLimiter {

    private final Executor limiter =
            Executors.newSingleThreadExecutor(new ExecutorThreadFactory("flink-snapshot-rate-limiter"));
    private final RateLimiter rateLimiter;

    private int getTokenCountAtOnce;

    public MysqlCdcGuavaRateLimiter(double recordsPerSecond, int parallelism) {
        this.rateLimiter = RateLimiter.create(recordsPerSecond);
        this.getTokenCountAtOnce = parallelism;
    }

    /**
     * Due to the CDC feature, the degree of parallelism will change to 1 after the checkpoint is completed,
     * so you need to reset the speed limiter.
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        getTokenCountAtOnce = 1;
    }

    public void getAcquire() {
        rateLimiter.acquire(getTokenCountAtOnce);
    }

    @Override
    public CompletionStage<Void> acquire() {
        return CompletableFuture.runAsync(this::getAcquire, limiter);
    }
}
