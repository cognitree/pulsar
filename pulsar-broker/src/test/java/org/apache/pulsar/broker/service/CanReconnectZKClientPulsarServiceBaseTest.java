/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.service;

import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import io.netty.channel.Channel;
import java.net.URL;
import java.nio.channels.SelectionKey;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.policies.data.TopicType;
import org.apache.pulsar.metadata.api.extended.SessionEvent;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.tests.TestRetrySupport;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.pulsar.zookeeper.ZookeeperServerTest;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.awaitility.reflect.WhiteboxImpl;

@Slf4j
public abstract class CanReconnectZKClientPulsarServiceBaseTest extends TestRetrySupport {
    protected final String defaultTenant = "public";
    protected final String defaultNamespace = defaultTenant + "/default";
    private static final String caCertPath = Resources.getResource("certificate-authority/certs/ca.cert.pem")
            .getPath();
    private static final String brokerCertPath =
            Resources.getResource("certificate-authority/server-keys/broker.cert.pem").getPath();
    private static final String brokerKeyPath =
            Resources.getResource("certificate-authority/server-keys/broker.key-pk8.pem").getPath();
    protected int numberOfBookies = 3;
    protected final String clusterName = "r1";
    protected URL url;
    protected URL urlTls;
    protected ServiceConfiguration config = new ServiceConfiguration();
    protected ZookeeperServerTest brokerConfigZk;
    protected LocalBookkeeperEnsemble bkEnsemble;
    protected PulsarService pulsar;
    protected BrokerService broker;
    protected PulsarAdmin admin;
    protected PulsarClient client;
    protected ZooKeeper localZkOfBroker;
    protected volatile SessionEvent sessionEvent;
    protected Object localMetaDataStoreClientCnx;
    protected final AtomicBoolean connectionTerminationThreadKeepRunning = new AtomicBoolean();
    private volatile Thread connectionTerminationThread;

    protected void startZKAndBK() throws Exception {
        // Start ZK.
        brokerConfigZk = new ZookeeperServerTest(0);
        brokerConfigZk.start();

        // Start BK.
        bkEnsemble = new LocalBookkeeperEnsemble(numberOfBookies, 0, () -> 0);
        bkEnsemble.start();
    }

    protected void startBrokers() throws Exception {
        // Start brokers.
        setConfigDefaults(config, clusterName, bkEnsemble, brokerConfigZk);
        pulsar = new PulsarService(config);
        pulsar.start();
        broker = pulsar.getBrokerService();
        ZKMetadataStore zkMetadataStore = (ZKMetadataStore) pulsar.getLocalMetadataStore();
        localZkOfBroker = zkMetadataStore.getZkClient();
        zkMetadataStore.registerSessionListener(n -> {
            log.info("Received session event: {}", n);
            sessionEvent = n;
        });
        ClientCnxn cnxn = WhiteboxImpl.getInternalState(localZkOfBroker, "cnxn");
        Object sendThread = WhiteboxImpl.getInternalState(cnxn, "sendThread");
        localMetaDataStoreClientCnx = WhiteboxImpl.getInternalState(sendThread, "clientCnxnSocket");

        url = new URL(pulsar.getWebServiceAddress());
        urlTls = new URL(pulsar.getWebServiceAddressTls());
        admin = PulsarAdmin.builder().serviceHttpUrl(url.toString()).build();
        client = PulsarClient.builder().serviceUrl(url.toString()).build();
    }

    protected void startLocalMetadataStoreConnectionTermination() throws Exception {
        if (!connectionTerminationThreadKeepRunning.compareAndSet(false, true)) {
            throw new RuntimeException("Local metadata store connection is already being terminated");
        }
        CompletableFuture<Void> future = new CompletableFuture<>();
        if (localMetaDataStoreClientCnx.getClass().getSimpleName().equals("ClientCnxnSocketNIO")) {
            startNIOImplTermination(future);
        } else {
            // ClientCnxnSocketNetty.
            startNettyImplTermination(future);
        }
        // wait until connection is closed at least once
        future.get();
    }

    private void startNIOImplTermination(CompletableFuture<Void> future) {
        connectionTerminationThread = new Thread(() -> {
            while (connectionTerminationThreadKeepRunning.get()) {
                try {
                    SelectionKey sockKey = WhiteboxImpl.getInternalState(localMetaDataStoreClientCnx, "sockKey");
                    if (sockKey != null) {
                        sockKey.channel().close();
                        future.complete(null);
                    }
                    // Prevents high cpu usage.
                    Thread.sleep(5);
                } catch (Exception e) {
                    log.error("Try close the ZK connection of local metadata store failed: {}", e.toString());
                }
            }
        });
        connectionTerminationThread.start();
    }

    private void startNettyImplTermination(CompletableFuture<Void> future) {
        connectionTerminationThread = new Thread(() -> {
            while (connectionTerminationThreadKeepRunning.get()) {
                try {
                    Channel channel = WhiteboxImpl.getInternalState(localMetaDataStoreClientCnx, "channel");
                    if (channel != null) {
                        channel.close();
                        future.complete(null);
                    }
                    // Prevents high cpu usage.
                    Thread.sleep(5);
                } catch (Exception e) {
                    log.error("Try close the ZK connection of local metadata store failed: {}", e.toString());
                }
            }
        });
        connectionTerminationThread.start();
    }

    protected void stopLocalMetadataStoreConnectionTermination() throws InterruptedException {
        connectionTerminationThreadKeepRunning.set(false);
        if (connectionTerminationThread != null) {
            // Wait for the reconnect thread to finish.
            connectionTerminationThread.join();
            connectionTerminationThread = null;
        }
        Awaitility.await().until(() -> SessionEvent.Reconnected.equals(sessionEvent));
    }

    protected void createDefaultTenantsAndClustersAndNamespace() throws Exception {
        admin.clusters().createCluster(clusterName, ClusterData.builder()
                .serviceUrl(url.toString())
                .serviceUrlTls(urlTls.toString())
                .brokerServiceUrl(pulsar.getBrokerServiceUrl())
                .brokerServiceUrlTls(pulsar.getBrokerServiceUrlTls())
                .brokerClientTlsEnabled(false)
                .build());

        admin.tenants().createTenant(defaultTenant, new TenantInfoImpl(Collections.emptySet(),
                Sets.newHashSet(clusterName)));

        admin.namespaces().createNamespace(defaultNamespace, Sets.newHashSet(clusterName));
    }

    @Override
    protected void setup() throws Exception {
        incrementSetupNumber();

        log.info("--- Starting OneWayReplicatorTestBase::setup ---");

        startZKAndBK();

        startBrokers();

        createDefaultTenantsAndClustersAndNamespace();

        Thread.sleep(100);
        log.info("--- OneWayReplicatorTestBase::setup completed ---");
    }

    private void setConfigDefaults(ServiceConfiguration config, String clusterName,
                                   LocalBookkeeperEnsemble bookkeeperEnsemble, ZookeeperServerTest brokerConfigZk) {
        config.setClusterName(clusterName);
        config.setAdvertisedAddress("localhost");
        config.setWebServicePort(Optional.of(0));
        config.setWebServicePortTls(Optional.of(0));
        config.setMetadataStoreUrl("zk:127.0.0.1:" + bookkeeperEnsemble.getZookeeperPort());
        config.setConfigurationMetadataStoreUrl("zk:127.0.0.1:" + brokerConfigZk.getZookeeperPort() + "/foo");
        config.setBrokerDeleteInactiveTopicsEnabled(false);
        config.setBrokerDeleteInactiveTopicsFrequencySeconds(60);
        config.setBrokerShutdownTimeoutMs(0L);
        config.setLoadBalancerOverrideBrokerNicSpeedGbps(Optional.of(1.0d));
        config.setBrokerServicePort(Optional.of(0));
        config.setBrokerServicePortTls(Optional.of(0));
        config.setBacklogQuotaCheckIntervalInSeconds(5);
        config.setDefaultNumberOfNamespaceBundles(1);
        config.setAllowAutoTopicCreationType(TopicType.NON_PARTITIONED);
        config.setEnableReplicatedSubscriptions(true);
        config.setReplicatedSubscriptionsSnapshotFrequencyMillis(1000);
        config.setTlsTrustCertsFilePath(caCertPath);
        config.setTlsCertificateFilePath(brokerCertPath);
        config.setTlsKeyFilePath(brokerKeyPath);
    }

    @Override
    protected void cleanup() throws Exception {
        markCurrentSetupNumberCleaned();
        log.info("--- Shutting down ---");

        stopLocalMetadataStoreConnectionTermination();

        // Stop brokers.
        if (client != null) {
            client.close();
            client = null;
        }
        if (admin != null) {
            admin.close();
            admin = null;
        }
        if (pulsar != null) {
            pulsar.close();
            pulsar = null;
        }

        // Stop ZK and BK.
        if (bkEnsemble != null) {
            bkEnsemble.stop();
            bkEnsemble = null;
        }
        if (brokerConfigZk != null) {
            brokerConfigZk.stop();
            brokerConfigZk = null;
        }

        // Reset configs.
        config = new ServiceConfiguration();
    }
}
