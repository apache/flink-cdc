/*
 * Copyright 2022 Ververica Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.tikv.cdc;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.common.region.TiRegion;
import org.tikv.common.util.FastByteComparisons;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Cdcpb;
import org.tikv.kvproto.Cdcpb.ChangeDataEvent;
import org.tikv.kvproto.Cdcpb.ChangeDataRequest;
import org.tikv.kvproto.Cdcpb.Event.LogType;
import org.tikv.kvproto.Cdcpb.Event.Row;
import org.tikv.kvproto.Cdcpb.Header;
import org.tikv.kvproto.Cdcpb.ResolvedTs;
import org.tikv.kvproto.ChangeDataGrpc;
import org.tikv.kvproto.ChangeDataGrpc.ChangeDataStub;
import org.tikv.kvproto.Coprocessor.KeyRange;
import org.tikv.shade.io.grpc.ManagedChannel;
import org.tikv.shade.io.grpc.stub.StreamObserver;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Copied from https://github.com/tikv/client-java project to fix
 * https://github.com/tikv/client-java/issues/600 for 3.2.0 version.
 */
public class RegionCDCClient implements AutoCloseable, StreamObserver<ChangeDataEvent> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RegionCDCClient.class);
    private static final AtomicLong REQ_ID_COUNTER = new AtomicLong(0);
    private static final Set<LogType> ALLOWED_LOGTYPE =
            ImmutableSet.of(LogType.PREWRITE, LogType.COMMIT, LogType.COMMITTED, LogType.ROLLBACK);

    private TiRegion region;
    private final KeyRange keyRange;
    private final KeyRange regionKeyRange;
    private final ManagedChannel channel;
    private final ChangeDataStub asyncStub;
    private final Consumer<CDCEvent> eventConsumer;
    private final CDCConfig config;
    private final Predicate<Row> rowFilter;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final boolean started = false;

    private long resolvedTs = 0;

    public RegionCDCClient(
            final TiRegion region,
            final KeyRange keyRange,
            final ManagedChannel channel,
            final Consumer<CDCEvent> eventConsumer,
            final CDCConfig config) {
        this.region = region;
        this.keyRange = keyRange;
        this.channel = channel;
        this.asyncStub = ChangeDataGrpc.newStub(channel);
        this.eventConsumer = eventConsumer;
        this.config = config;

        this.regionKeyRange =
                KeyRange.newBuilder()
                        .setStart(region.getStartKey())
                        .setEnd(region.getEndKey())
                        .build();

        this.rowFilter =
                regionEnclosed()
                        ? ((row) -> true)
                        : new Predicate<Row>() {
                            final byte[] buffer = new byte[config.getMaxRowKeySize()];

                            final byte[] start = keyRange.getStart().toByteArray();
                            final byte[] end = keyRange.getEnd().toByteArray();

                            @Override
                            public boolean test(final Row row) {
                                final int len = row.getKey().size();
                                row.getKey().copyTo(buffer, 0);
                                return (FastByteComparisons.compareTo(
                                                        buffer, 0, len, start, 0, start.length)
                                                >= 0)
                                        && (FastByteComparisons.compareTo(
                                                        buffer, 0, len, end, 0, end.length)
                                                < 0);
                            }
                        };
    }

    public synchronized void start(final long startTs) {
        Preconditions.checkState(!started, "RegionCDCClient has already started");
        resolvedTs = startTs;
        running.set(true);
        LOGGER.info("start streaming region: {}, running: {}", region.getId(), running.get());
        final ChangeDataRequest request =
                ChangeDataRequest.newBuilder()
                        .setRequestId(REQ_ID_COUNTER.incrementAndGet())
                        .setHeader(Header.newBuilder().setTicdcVersion("5.0.0").build())
                        .setRegionId(region.getId())
                        .setCheckpointTs(startTs)
                        .setStartKey(keyRange.getStart())
                        .setEndKey(keyRange.getEnd())
                        .setRegionEpoch(region.getRegionEpoch())
                        .setExtraOp(config.getExtraOp())
                        .build();
        final StreamObserver<ChangeDataRequest> requestObserver = asyncStub.eventFeed(this);
        HashMap<String, Object> params = new HashMap<>();
        params.put("requestId", request.getRequestId());
        params.put("header", request.getHeader());
        params.put("regionId", request.getRegionId());
        params.put("checkpointTs", request.getCheckpointTs());
        params.put("startKey", request.getStartKey().toString());
        params.put("endKey", request.getEndKey().toString());
        params.put("regionEpoch", request.getRegionEpoch());
        params.put("extraOp", request.getExtraOp());
        requestObserver.onNext(request);
    }

    public TiRegion getRegion() {
        return region;
    }

    public void setRegion(TiRegion region) {
        this.region = region;
    }

    public KeyRange getKeyRange() {
        return keyRange;
    }

    public KeyRange getRegionKeyRange() {
        return regionKeyRange;
    }

    public boolean regionEnclosed() {
        return KeyRangeUtils.makeRange(keyRange.getStart(), keyRange.getEnd())
                .encloses(
                        KeyRangeUtils.makeRange(
                                regionKeyRange.getStart(), regionKeyRange.getEnd()));
    }

    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void close() throws Exception {
        LOGGER.info("close (region: {})", region.getId());
        running.set(false);
        // fix: close grpc channel will make client threadpool shutdown.
        /*
        synchronized (this) {
          channel.shutdown();
        }
        try {
          LOGGER.debug("awaitTermination (region: {})", region.getId());
          channel.awaitTermination(60, TimeUnit.SECONDS);
        } catch (final InterruptedException e) {
          LOGGER.error("Failed to shutdown channel(regionId: {})", region.getId());
          Thread.currentThread().interrupt();
          synchronized (this) {
            channel.shutdownNow();
          }
        }
        */
        LOGGER.info("terminated (region: {})", region.getId());
    }

    @Override
    public void onCompleted() {
        // should never been called
        onError(new IllegalStateException("RegionCDCClient should never complete"));
    }

    @Override
    public void onError(final Throwable error) {
        onError(error, this.resolvedTs);
    }

    private void onError(final Throwable error, long resolvedTs) {
        LOGGER.error(
                "region CDC error: region: {}, resolvedTs:{}, error: {}",
                region.getId(),
                resolvedTs,
                error);
        running.set(false);
        eventConsumer.accept(CDCEvent.error(region.getId(), error, resolvedTs));
    }

    @Override
    public void onNext(final ChangeDataEvent event) {
        try {
            if (running.get()) {
                // fix: miss to process error event
                onErrorEventHandle(event);
                event.getEventsList().stream()
                        .flatMap(ev -> ev.getEntries().getEntriesList().stream())
                        .filter(row -> ALLOWED_LOGTYPE.contains(row.getType()))
                        .filter(this.rowFilter)
                        .map(row -> CDCEvent.rowEvent(region.getId(), row))
                        .forEach(this::submitEvent);

                if (event.hasResolvedTs()) {
                    final ResolvedTs resolvedTs = event.getResolvedTs();
                    this.resolvedTs = resolvedTs.getTs();
                    if (resolvedTs.getRegionsList().indexOf(region.getId()) >= 0) {
                        submitEvent(CDCEvent.resolvedTsEvent(region.getId(), resolvedTs.getTs()));
                    }
                }
            }
        } catch (final Exception e) {
            onError(e, resolvedTs);
        }
    }

    // error event handle
    private void onErrorEventHandle(final ChangeDataEvent event) {
        List<Cdcpb.Event> errorEvents =
                event.getEventsList().stream()
                        .filter(errEvent -> errEvent.hasError())
                        .collect(Collectors.toList());
        if (errorEvents != null && errorEvents.size() > 0) {
            onError(
                    new RuntimeException(
                            "regionCDC error:" + errorEvents.get(0).getError().toString()),
                    this.resolvedTs);
        }
    }

    private void submitEvent(final CDCEvent event) {
        LOGGER.debug("submit event: {}", event);
        eventConsumer.accept(event);
    }
}
