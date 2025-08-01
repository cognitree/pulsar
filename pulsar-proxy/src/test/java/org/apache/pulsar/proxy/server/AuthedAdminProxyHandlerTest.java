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
package org.apache.pulsar.proxy.server;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Optional;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.authentication.AuthenticationProviderTls;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.broker.resources.PulsarResources;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.impl.auth.AuthenticationTls;
import org.apache.pulsar.common.configuration.PulsarConfigurationLoader;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.metadata.impl.ZKMetadataStore;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;
import org.apache.pulsar.policies.data.loadbalancer.LoadReport;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AuthedAdminProxyHandlerTest extends MockedPulsarServiceBaseTest {
    private static final Logger LOG = LoggerFactory.getLogger(AuthedAdminProxyHandlerTest.class);

    private ProxyConfiguration proxyConfig = new ProxyConfiguration();
    private Authentication proxyClientAuthentication;
    private WebServer webServer;
    private BrokerDiscoveryProvider discoveryProvider;
    private PulsarResources resource;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        // enable tls and auth&auth at broker
        conf.setAuthenticationEnabled(true);
        conf.setAuthorizationEnabled(true);
        conf.setBrokerServicePortTls(Optional.of(0));
        conf.setWebServicePortTls(Optional.of(0));
        conf.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);
        conf.setTlsCertificateFilePath(BROKER_CERT_FILE_PATH);
        conf.setTlsKeyFilePath(BROKER_KEY_FILE_PATH);
        conf.setTlsAllowInsecureConnection(false);
        conf.setSuperUserRoles(ImmutableSet.of("admin"));
        conf.setProxyRoles(ImmutableSet.of("proxy"));
        conf.setAuthenticationProviders(ImmutableSet.of(AuthenticationProviderTls.class.getName()));
        conf.setNumExecutorThreadPoolSize(5);
        conf.setHttpMaxRequestHeaderSize(20000);

        super.internalSetup();

        // start proxy service
        proxyConfig.setAuthenticationEnabled(true);
        proxyConfig.setAuthorizationEnabled(true);
        proxyConfig.setServicePort(Optional.of(0));
        proxyConfig.setBrokerProxyAllowedTargetPorts("*");
        proxyConfig.setServicePortTls(Optional.of(0));
        proxyConfig.setWebServicePort(Optional.of(0));
        proxyConfig.setWebServicePortTls(Optional.of(0));
        proxyConfig.setTlsEnabledWithBroker(true);
        proxyConfig.setHttpMaxRequestHeaderSize(20000);
        proxyConfig.setClusterName(configClusterName);

        // enable tls and auth&auth at proxy
        proxyConfig.setTlsCertificateFilePath(PROXY_CERT_FILE_PATH);
        proxyConfig.setTlsKeyFilePath(PROXY_KEY_FILE_PATH);
        proxyConfig.setTlsTrustCertsFilePath(CA_CERT_FILE_PATH);

        proxyConfig.setBrokerClientAuthenticationPlugin(AuthenticationTls.class.getName());
        proxyConfig.setBrokerClientAuthenticationParameters(
                String.format("tlsCertFile:%s,tlsKeyFile:%s",
                              getTlsFileForClient("proxy.cert"), getTlsFileForClient("proxy.key-pk8")));
        proxyConfig.setBrokerClientTrustCertsFilePath(CA_CERT_FILE_PATH);
        proxyConfig.setAuthenticationProviders(ImmutableSet.of(AuthenticationProviderTls.class.getName()));

        proxyClientAuthentication = AuthenticationFactory.create(proxyConfig.getBrokerClientAuthenticationPlugin(),
                proxyConfig.getBrokerClientAuthenticationParameters());
        proxyClientAuthentication.start();

        resource = new PulsarResources(registerCloseable(new ZKMetadataStore(mockZooKeeper)),
                registerCloseable(new ZKMetadataStore(mockZooKeeperGlobal)));
        webServer = new WebServer(proxyConfig, new AuthenticationService(
                                          PulsarConfigurationLoader.convertFrom(proxyConfig)));
        discoveryProvider = spy(registerCloseable(new BrokerDiscoveryProvider(proxyConfig, resource)));
        LoadManagerReport report = new LoadReport(brokerUrl.toString(), brokerUrlTls.toString(), null, null);
        doReturn(report).when(discoveryProvider).nextBroker();

        ServletHolder servletHolder =
                new ServletHolder(new AdminProxyHandler(proxyConfig, discoveryProvider, proxyClientAuthentication));
        webServer.addServlet("/admin", servletHolder);
        webServer.addServlet("/lookup", servletHolder);

        // start web-service
        webServer.start();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        webServer.stop();
        if (proxyClientAuthentication != null) {
            proxyClientAuthentication.close();
        }
        super.internalCleanup();
    }

    PulsarAdmin getDirectToBrokerAdminClient(String user) throws Exception {
        return PulsarAdmin.builder()
            .serviceHttpUrl(brokerUrlTls.toString())
            .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
            .allowTlsInsecureConnection(false)
            .authentication(AuthenticationTls.class.getName(),
                    ImmutableMap.of("tlsCertFile", getTlsFileForClient(user + ".cert"),
                                    "tlsKeyFile", getTlsFileForClient(user + ".key-pk8")))
            .build();
    }

    PulsarAdmin getAdminClient(String user) throws Exception {
        return PulsarAdmin.builder()
            .serviceHttpUrl("https://localhost:" + webServer.getListenPortHTTPS().get())
            .tlsTrustCertsFilePath(CA_CERT_FILE_PATH)
            .allowTlsInsecureConnection(false)
            .authentication(AuthenticationTls.class.getName(),
                    ImmutableMap.of("tlsCertFile", getTlsFileForClient(user + ".cert"),
                                    "tlsKeyFile", getTlsFileForClient(user + ".key-pk8")))
            .build();
    }

    @Test
    public void testAuthenticatedProxyAsNonAdmin() throws Exception {
        try (PulsarAdmin brokerAdmin = getDirectToBrokerAdminClient("admin");
             PulsarAdmin proxyAdmin = getAdminClient("admin");
             PulsarAdmin user1Admin = getAdminClient("user1")) {
            try {
                proxyAdmin.tenants().getTenants();
                Assert.fail("Shouldn't be able to do superuser operation, since proxy isn't super");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }

            brokerAdmin.clusters().createCluster(configClusterName, ClusterData.builder()
                    .serviceUrl(brokerUrl.toString()).build());

            brokerAdmin.tenants().createTenant("tenant1",
                                               new TenantInfoImpl(ImmutableSet.of("user1"),
                                                              ImmutableSet.of(configClusterName)));
            brokerAdmin.namespaces().createNamespace("tenant1/ns1");
            Assert.assertEquals(ImmutableSet.of("tenant1/ns1"), brokerAdmin.namespaces().getNamespaces("tenant1"));
            try {
                user1Admin.namespaces().getNamespaces("tenant1");
                Assert.fail("Shouldn't have access to namespace since proxy doesn't");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }
            brokerAdmin.tenants().updateTenant("tenant1",
                                               new TenantInfoImpl(ImmutableSet.of("proxy"),
                                                              ImmutableSet.of(configClusterName)));
            try {
                user1Admin.namespaces().getNamespaces("tenant1");
                Assert.fail("Shouldn't have access to namespace since user1 doesn't");
            } catch (PulsarAdminException.NotAuthorizedException e) {
                // expected
            }
            brokerAdmin.tenants().updateTenant("tenant1",
                                               new TenantInfoImpl(ImmutableSet.of("user1", "proxy"),
                                                              ImmutableSet.of(configClusterName)));
            Assert.assertEquals(ImmutableSet.of("tenant1/ns1"), user1Admin.namespaces().getNamespaces("tenant1"));
        }
    }

    @Test
    public void testAuthenticatedRequestWithLongUri() throws Exception {
        try (PulsarAdmin user1Admin = getAdminClient("user1");
             PulsarAdmin brokerAdmin = getDirectToBrokerAdminClient("admin")) {
            StringBuilder longTenant = new StringBuilder("tenant");
            for (int i = 10 * 1024; i > 0; i = i - 4) {
                longTenant.append("_abc");
            }
            try {
                brokerAdmin.namespaces().getNamespaces(longTenant.toString());
                Assert.fail("expect error: Tenant not found");
            } catch (Exception ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException);
                PulsarAdminException pulsarAdminException = (PulsarAdminException) ex;
                Assert.assertEquals(pulsarAdminException.getStatusCode(), 404);
            }
            try {
                user1Admin.namespaces().getNamespaces(longTenant.toString());
                Assert.fail("expect error: Tenant not found");
            } catch (Exception ex) {
                Assert.assertTrue(ex instanceof PulsarAdminException);
                PulsarAdminException pulsarAdminException = (PulsarAdminException) ex;
                Assert.assertEquals(pulsarAdminException.getStatusCode(), 404);
            }
        }
    }
}
