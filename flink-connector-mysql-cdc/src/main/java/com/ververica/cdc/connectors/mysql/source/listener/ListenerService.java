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

package com.ververica.cdc.connectors.mysql.source.listener;

import org.apache.flink.annotation.Experimental;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/** This service can be used to send notification to available and enabled channels. */
@Experimental
public class ListenerService implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(ListenerService.class);

    private static final String CLASS_NAME = "listener.class";

    private final ExecutorService executorService;

    private final List<ExternalSystemListener> externalSystemListeners;

    public ListenerService(Properties listenerProperties) {
        externalSystemListeners = new ArrayList<>();

        String classes = listenerProperties.getProperty(CLASS_NAME);
        if (classes != null) {
            String[] classNames = classes.split(",");
            for (String className : classNames) {
                try {
                    className = className.trim();
                    Class cls = Class.forName(className);
                    ExternalSystemListener externalSystemListener =
                            (ExternalSystemListener) cls.newInstance();
                    externalSystemListener.init(listenerProperties);
                    externalSystemListeners.add(externalSystemListener);
                    LOG.info("succeeded to create instance of {}", className);
                } catch (ClassNotFoundException
                        | InstantiationException
                        | IllegalAccessException e) {
                    LOG.warn("failed to create instance of {}, skip it.", className);
                }
            }
        }

        /* Avoid wasting resources */
        if (externalSystemListeners.isEmpty()) {
            executorService = null;
        } else {
            ThreadFactory threadFactory =
                    new ThreadFactoryBuilder().setNameFormat("listener_service").build();
            executorService = Executors.newSingleThreadExecutor(threadFactory);
        }
    }

    /** notify all external system listeners. */
    public void notifyAllListeners(
            final AssignerStatus assignerStatus, AbstractListenerMessage listenerMessage) {
        if (externalSystemListeners.isEmpty() || executorService == null) {
            return;
        }

        executorService.submit(
                () -> {
                    long startTime = System.currentTimeMillis();
                    externalSystemListeners.forEach(
                            externalSystemListener -> {
                                try {
                                    externalSystemListener.send(assignerStatus, listenerMessage);
                                } catch (Exception e) {
                                    LOG.warn(
                                            "failed to send notification to {}",
                                            externalSystemListener.name());
                                }
                            });
                    LOG.debug(
                            "cost {}ms to notify all listeners",
                            System.currentTimeMillis() - startTime);
                });
    }

    public void close() {
        if (executorService != null) {
            executorService.shutdown();
        }
        externalSystemListeners.forEach(ExternalSystemListener::close);
    }
}
