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

import com.ververica.cdc.connectors.mysql.source.assigners.AssignerStatus;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ListenerService}. */
public class ListenerServiceTest {

    @Test
    public void testNotifyAllListeners() throws InterruptedException {
        Properties listenerProperties = new Properties();
        listenerProperties.setProperty(
                "listener.class", ExternalSystemListenerImplTest.class.getName());
        ListenerService listenerService = new ListenerService(listenerProperties);
        listenerService.notifyAllListeners(AssignerStatus.INITIAL_ASSIGNING_FINISHED);
        /* wait for async execution */
        Thread.sleep(200);
        listenerService.close();
        assertEquals(
                ExternalSystemListenerImplTest.assignerStatus,
                AssignerStatus.INITIAL_ASSIGNING_FINISHED);
    }

    static class ExternalSystemListenerImplTest implements ExternalSystemListener {

        public static AssignerStatus assignerStatus;

        @Override
        public String name() {
            return "test_listener";
        }

        @Override
        public void init(Properties properties) {
            assignerStatus = AssignerStatus.INITIAL_ASSIGNING;
        }

        @Override
        public void send(AssignerStatus assignerStatus) {
            ExternalSystemListenerImplTest.assignerStatus = assignerStatus;
        }

        @Override
        public void close() {}
    }
}
