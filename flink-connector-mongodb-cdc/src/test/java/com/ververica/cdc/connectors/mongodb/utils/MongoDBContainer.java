/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ververica.cdc.connectors.mongodb.utils;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/** Mongodb test replica container. */
public class MongoDBContainer extends GenericContainer<MongoDBContainer> {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBContainer.class);

    private static final String REPLICA_SET_NAME = "rs0";

    private static final String DOCKER_IMAGE_NAME = "mongo:5.0.2";

    private static final int MONGODB_PORT = 27017;

    public MongoDBContainer() {
        super(DOCKER_IMAGE_NAME);
        withExposedPorts(MONGODB_PORT);
        withCommand("--replSet", REPLICA_SET_NAME);
        waitingFor(Wait.forLogMessage(".*[Ww]aiting for connections.*", 1));
    }

    public String getLocalHost() {
        try {
            Enumeration<NetworkInterface> networkInterfaces =
                    NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface ni = networkInterfaces.nextElement();

                if (ni.isLoopback() || ni.isVirtual() || ni.isPointToPoint() || !ni.isUp()) {
                    continue;
                }

                Enumeration<InetAddress> addresses = ni.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress address = addresses.nextElement();
                    if (address.isSiteLocalAddress()) {
                        return address.getHostAddress();
                    }
                }
            }

            return InetAddress.getLocalHost().getHostAddress();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot get local ip address", e);
        }
    }

    public String getConnectionString(String username, String password) {
        return String.format(
                "mongodb://%s:%s@%s:%d",
                username, password, getLocalHost(), getMappedPort(MONGODB_PORT));
    }

    public String getHostAndPort() {
        return String.format("%s:%s", getLocalHost(), getMappedPort(MONGODB_PORT));
    }

    public void executeCommand(String command) {
        try {
            LOG.info("Executing mongo command: {}", command);
            ExecResult execResult = execInContainer("mongo", "--eval", command);
            LOG.info(execResult.getStdout());
            if (execResult.getExitCode() != 0) {
                throw new IllegalStateException(
                        "Execute mongo command failed " + execResult.getStdout());
            }
        } catch (InterruptedException | IOException e) {
            throw new IllegalStateException("Execute mongo command failed", e);
        }
    }

    @Override
    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        initReplicaSet();
    }

    private void initReplicaSet() {
        LOG.info("Initializing a single node node replica set...");
        executeCommand(
                String.format(
                        "rs.initiate({ _id : '%s', members: [{ _id: 0, host: '%s:%d'}]})",
                        REPLICA_SET_NAME, getLocalHost(), getMappedPort(MONGODB_PORT)));

        LOG.info("Waiting for single node node replica set initialized...");
        executeCommand(
                String.format(
                        "var attempt = 0; "
                                + "while"
                                + "(%s) "
                                + "{ "
                                + "if (attempt > %d) {quit(1);} "
                                + "print('%s ' + attempt); sleep(100);  attempt++; "
                                + " }",
                        "db.runCommand( { isMaster: 1 } ).ismaster==false",
                        60,
                        "An attempt to await for a single node replica set initialization:"));
    }
}
