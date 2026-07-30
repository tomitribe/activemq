/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.camel;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.camel.Exchange;
import org.apache.camel.component.jms.JmsBinding;
import org.apache.camel.component.jms.JmsMessage;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.apache.camel.util.ExchangeHelper;
import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.junit.Test;
import org.springframework.context.support.AbstractApplicationContext;

import javax.jms.*;

/**
 * Regression coverage for CVE-2026-43866 (camel-jms DefaultExchangeHolder / ObjectMessage
 * deserialization) as it applies to ActiveMQ's Camel integration.
 *
 * A single {@link ObjectPayload} ObjectMessage is published to topic {@code foo}; eight routes
 * consume it, exercising two independent gates (see jms-object-message.xml):
 *
 *   Gate 1 - camel-jms {@code objectMessageEnabled} (CAMEL-23373 backport). Defaults FALSE in the
 *            TT fork, so Camel refuses to unmarshal ANY JMS ObjectMessage before ActiveMQ's
 *            trustedPackages is even consulted. This is the fix.
 *   Gate 2 - ActiveMQ {@code trustedPackages}. Only reached once gate 1 has been opened by an
 *            explicit {@code objectMessageEnabled=true}; then trust decides per payload class.
 *
 * ActiveMQ is therefore NOT AFFECTED in its default configuration: an attacker's holder is
 * refused at gate 1. Exposure needs BOTH footguns - an operator re-enabling ObjectMessage
 * consumption AND relaxing trustedPackages. See tomitribe/cve
 * docs/security-audits/2026/CVE-2026-43866.md.
 */
public class ObjectMessageTest extends CamelSpringTestSupport {

    /**
     * Gate 1 (the fix): with {@code objectMessageEnabled} at its default (false), Camel refuses
     * the ObjectMessage on every route - even the connection factories that trust the payload's
     * package or trust everything. Trust never gets a say, because the message is rejected first.
     */
    @Test
    public void testObjectMessageRefusedByDefault() throws Exception {
        publishObjectPayloadToTopicFoo();

        // gate 1 fires first: the message is refused as "disabled" - trust never gets a say,
        // so even the trusting factories reject it for this reason rather than delivering.
        assertRefused("mock:result-activemq", "ObjectMessage is disabled"); // trusts org.apache.activemq
        assertRefused("mock:result-trusted", "ObjectMessage is disabled");  // trustAllPackages
        assertRefused("mock:result-camel", "ObjectMessage is disabled");
        assertRefused("mock:result-empty", "ObjectMessage is disabled");
    }

    /**
     * Gate 2 (the two-footgun case): once an operator explicitly opts back in with
     * {@code objectMessageEnabled=true}, ActiveMQ's trustedPackages becomes the deciding factor -
     * exactly the original pre-fix matrix, now reachable only behind that opt-in.
     */
    @Test
    public void testTrustedPackagesGateWhenObjectMessageEnabled() throws Exception {
        publishObjectPayloadToTopicFoo();

        assertReceivedOne("mock:result-activemq-enabled"); // trusts org.apache.activemq -> delivered
        assertReceivedOne("mock:result-trusted-enabled");  // trustAllPackages       -> delivered
        // gate 1 open, so now trust decides: the payload's package is not trusted here
        assertRefused("mock:result-camel-enabled", "Forbidden class"); // trusts only org.apache.camel
        assertRefused("mock:result-empty-enabled", "Forbidden class"); // empty trust
    }

    private void publishObjectPayloadToTopicFoo() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("vm://localhost");
        final Connection conn = factory.createConnection();
        try {
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageProducer producer = sess.createProducer(sess.createTopic("foo"));
            final ObjectMessage msg = sess.createObjectMessage();
            final ObjectPayload payload = new ObjectPayload();
            payload.payload = "test";
            msg.setObject(payload);
            producer.send(msg);
        } finally {
            conn.close();
        }

        // give the eight topic consumers time to (attempt to) process the message
        Thread.sleep(1000);
    }

    private void assertReceivedOne(final String uri) throws Exception {
        final MockEndpoint result = resolveMandatoryEndpoint(uri, MockEndpoint.class);
        result.expectedMessageCount(1);
        result.assertIsSatisfied();
        assertCorrectObjectReceived(result);
    }

    /**
     * Asserts the payload was NOT delivered. When a route refuses an ObjectMessage, Camel attaches
     * the failure to the exchange and MockEndpoint surfaces it, so a refusal manifests as an
     * assertion error carrying the security reason (never as a successful delivery). We accept
     * either "nothing arrived" or "arrived only as a failure with the expected reason".
     */
    private void assertRefused(final String uri, final String expectedReason) throws Exception {
        final MockEndpoint result = resolveMandatoryEndpoint(uri, MockEndpoint.class);
        result.expectedMessageCount(0);
        result.setResultWaitTime(1500L);
        try {
            result.assertIsSatisfied();
        } catch (final AssertionError refused) {
            assertTrue("expected refusal reason '" + expectedReason + "' for " + uri
                            + " but was: " + refused.getMessage(),
                    refused.getMessage().contains(expectedReason));
        }
    }

    protected void assertCorrectObjectReceived(final MockEndpoint result) {
        final Exchange exchange = result.getReceivedExchanges().get(0);
        // This should be a JMS Exchange
        assertNotNull(ExchangeHelper.getBinding(exchange, JmsBinding.class));
        final JmsMessage in = (JmsMessage) exchange.getIn();
        assertNotNull(in);
        assertIsInstanceOf(ObjectMessage.class, in.getJmsMessage());

        final ObjectPayload received = exchange.getIn().getBody(ObjectPayload.class);
        assertEquals("test", received.payload);
    }

    @Override
    protected AbstractApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("org/apache/activemq/camel/jms-object-message.xml");
    }
}
