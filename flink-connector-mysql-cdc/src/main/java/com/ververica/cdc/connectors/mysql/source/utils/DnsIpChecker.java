/*
 * Copyright 2023 Ververica Inc.
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

package com.ververica.cdc.connectors.mysql.source.utils;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.mysql.source.events.DnsIpChangedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

/**
 * A DNSIpChecker check whether ip address in DNS has changed or not. If changed, throw *
 * FlinkRuntimeException
 */
public class DnsIpChecker {
    private static final Logger LOG = LoggerFactory.getLogger(DnsIpChecker.class);
    private int checkIntervalMs = 1000;
    private int logInterval = 60 * checkIntervalMs / 1000;
    private volatile boolean currentTaskRunning = false;
    private ExecutorService executorService;
    private final String fqdn;
    private final SourceReaderContext context;
    private final String initIp;
    private static final long DNS_CHECKER_CLOSE_TIMEOUT = 30L;
    private static final long DNS_CHECKER_CLOSE_MAX_TIMEOUT = 300L;

    public DnsIpChecker(int subTaskId, SourceReaderContext context, String fqdn) {
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("binlog-dns-checker-" + subTaskId).build();
        this.executorService = Executors.newSingleThreadExecutor(threadFactory);
        this.context = context;
        this.fqdn = fqdn;
        this.initIp = this.getIp();
    }

    public void run() {
        this.currentTaskRunning = true;
        this.executorService.execute(
                () -> {
                    try {
                        this.execute();
                    } catch (Exception e) {
                        this.context.sendSourceEventToCoordinator(new DnsIpChangedEvent());
                    }
                });
    }

    private void execute() throws Exception {
        LOG.info("Execute DNS IP Checker");
        String currentIpInDNS = "";
        Integer checkCnt = 0;
        while (this.currentTaskRunning) {
            checkCnt += 1;
            currentIpInDNS = this.getIp();

            if (currentIpInDNS.equals("")) {
                LOG.info("No stdout from dig");
            } else {
                if (checkCnt % logInterval == 1) {
                    LOG.info(
                            "[{}] Current IP address for hostname {} is {}",
                            checkCnt,
                            this.fqdn,
                            currentIpInDNS);
                }
                if (!currentIpInDNS.equals(this.initIp)) {
                    LOG.warn(
                            "IP address of {} has changed from {} to {} in DNS ",
                            this.fqdn,
                            this.initIp,
                            currentIpInDNS);
                    this.currentTaskRunning = false;
                    throw new FlinkRuntimeException("IP address has changed");
                }
            }
            Thread.sleep(checkIntervalMs);
        }
    }

    private String getIp() {
        String ipInDNS = "";
        try {
            ipInDNS = InetAddress.getByName(this.fqdn).getHostAddress();
        } catch (Exception e) {
            LOG.warn("Failed to get ip address from DNS");
        }
        return ipInDNS;
    }

    public void close() {
        if (this.executorService != null) {
            this.executorService.shutdownNow();
            try {
                if (!this.executorService.awaitTermination(
                        DNS_CHECKER_CLOSE_TIMEOUT, TimeUnit.SECONDS)) {
                    LOG.warn(
                            "Failed to close the dns checker in {} seconds.",
                            DNS_CHECKER_CLOSE_TIMEOUT);
                    this.executorService.shutdownNow();
                    if (!this.executorService.awaitTermination(
                            DNS_CHECKER_CLOSE_MAX_TIMEOUT, TimeUnit.SECONDS)) {
                        LOG.error("Still failed to close the dns checker.");
                    }
                }
            } catch (InterruptedException e) {
                LOG.error("Interrupted when closing the dns checker.");
            } finally {
                this.executorService = null;
            }
        }
    }
}
