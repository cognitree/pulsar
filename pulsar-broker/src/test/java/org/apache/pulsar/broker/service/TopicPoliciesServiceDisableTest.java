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

import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.TopicPolicies;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "broker")
public class TopicPoliciesServiceDisableTest extends MockedPulsarServiceBaseTest {

    private TopicPoliciesService systemTopicBasedTopicPoliciesService;

    @BeforeMethod
    @Override
    protected void setup() throws Exception {
        conf.setTopicLevelPoliciesEnabled(false);
        super.internalSetup();
        prepareData();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @Test
    public void testTopicLevelPoliciesDisabled() {
        try {
            systemTopicBasedTopicPoliciesService.updateTopicPoliciesAsync(TopicName.get("test"),
                    new TopicPolicies()).get();
        } catch (Exception e) {
            Assert.assertTrue(e.getCause() instanceof UnsupportedOperationException);
        }
    }

    private void prepareData() {
        systemTopicBasedTopicPoliciesService = pulsar.getTopicPoliciesService();
    }
}
