package org.apache.flink.cdc.connectors.mysql.rate;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The SnapshotGuavaRateLimiter class implements Flink's RateLimiter interface, which is used to limit the read rate of data sources.
 * It uses Guava's RateLimiter to control the number of requests allowed to pass per second,
 * ensuring that parallel tasks can adjust the rate limit after the checkpoint is completed.
 */
public class SnapshotGuavaRateLimiter implements org.apache.flink.api.connector.source.util.ratelimit.RateLimiter {
    private static final Logger LOG = LoggerFactory.getLogger(SnapshotGuavaRateLimiter.class);

    private final Executor limiter =
            Executors.newSingleThreadExecutor(new ExecutorThreadFactory("flink-snapshot-rate-limiter"));
    private RateLimiter rateLimiter;

    private final double maxPerSecond;
    private int parallelism;

    public SnapshotGuavaRateLimiter(double recordsPerSecond, int parallelism) {
        this.rateLimiter = RateLimiter.create(recordsPerSecond / parallelism);
        this.maxPerSecond = recordsPerSecond;
        this.parallelism = parallelism;
    }

    /**
     * Due to the CDC feature, the degree of parallelism will change to 1 after the checkpoint is completed,
     * so you need to reset the speed limiter.
     *
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        if (parallelism > 1) {
            LOG.info("检查点完成，重装限速器："+checkpointId);
            rateLimiter = RateLimiter.create(maxPerSecond / parallelism);
            parallelism = 1;
        }
    }

    @Override
    public CompletionStage<Void> acquire() {
        LOG.info("获取限速许可========================================================");
        return CompletableFuture.runAsync(rateLimiter::acquire, limiter);
    }
}
