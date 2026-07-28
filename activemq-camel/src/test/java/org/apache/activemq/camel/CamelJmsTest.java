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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.ProducerTemplate;
import org.apache.camel.test.spring.CamelSpringTestSupport;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 *
 */
public class CamelJmsTest extends CamelSpringTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(CamelJmsTest.class);

    protected String expectedBody = "<hello>world!</hello>";

    /**
     * CVE-2026-43866: the forked Camel 2.25.5-TT.x defaults {@code objectMessageEnabled=false},
     * so Camel refuses to unmarshal an inbound JMS ObjectMessage and nothing reaches the route.
     * (ActiveMQ is not affected in its default config: trustedPackages would reject the payload
     * too - this gate simply fires first. See tomitribe/cve
     * docs/security-audits/2026/CVE-2026-43866.md.)
     */
    @Test
    public void testSendingObjectMessageIsRefusedByDefault() throws Exception {
        final MockEndpoint result = resolveMandatoryEndpoint("mock:result", MockEndpoint.class);
        result.expectedMessageCount(0);
        result.setResultWaitTime(2000L);

        final Destination destination = getMandatoryBean(Destination.class, "sendTo");
        final ConnectionFactory factory = getMandatoryBean(ConnectionFactory.class, "connectionFactory");

        final Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(destination);

        final ObjectMessage message = session.createObjectMessage(expectedBody);
        message.setStringProperty("foo", "bar");
        producer.send(message);

        // the ObjectMessage must not reach the route; if it surfaces at all it is only as the
        // "disabled" refusal, never as a successful delivery
        try {
            result.assertIsSatisfied();
        } catch (final AssertionError refused) {
            assertTrue("expected ObjectMessage to be refused as disabled, but was: "
                            + refused.getMessage(),
                    refused.getMessage().contains("ObjectMessage is disabled"));
        }
        connection.close();
    }

    /**
     * Companion to {@link #testSendingObjectMessageIsRefusedByDefault()}: a plain TextMessage
     * is still delivered end to end, confirming the {@code objectMessageEnabled} gate is specific
     * to ObjectMessage and does not break ordinary JMS-to-Camel bridging.
     */
    @Test
    public void testSendingTextMessageIsReceivedByCamel() throws Exception {
        final MockEndpoint result = resolveMandatoryEndpoint("mock:result", MockEndpoint.class);
        result.expectedBodiesReceived(expectedBody);
        result.message(0).header("foo").isEqualTo("bar");

        final Destination destination = getMandatoryBean(Destination.class, "sendTo");
        final ConnectionFactory factory = getMandatoryBean(ConnectionFactory.class, "connectionFactory");

        final Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(destination);

        final TextMessage message = session.createTextMessage(expectedBody);
        message.setStringProperty("foo", "bar");
        producer.send(message);

        result.assertIsSatisfied();
        connection.close();

        LOG.info("Received message: " + result.getReceivedExchanges());
    }

    @Test
    public void testConsumingViaJMSReceivesMessageFromCamel() throws Exception {
        // lets create a message
        final Destination destination = getMandatoryBean(Destination.class, "consumeFrom");
        final ConnectionFactory factory = getMandatoryBean(ConnectionFactory.class, "connectionFactory");
        final ProducerTemplate template = getMandatoryBean(ProducerTemplate.class, "camelTemplate");
        assertNotNull("template is valid", template);

        final Connection connection = factory.createConnection();
        connection.start();
        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        LOG.info("Consuming from: " + destination);
        final MessageConsumer consumer = session.createConsumer(destination);

        // now lets send a message
        template.sendBody("seda:consumer", expectedBody);

        final Message message = consumer.receive(5000);
        assertNotNull("Should have received a message from destination: " + destination, message);

        final TextMessage textMessage = assertIsInstanceOf(TextMessage.class, message);
        assertEquals("Message body", expectedBody, textMessage.getText());

        LOG.info("Received message: " + message);
    }

    protected int getExpectedRouteCount() {
        return 0;
    }

    protected ClassPathXmlApplicationContext createApplicationContext() {
        return new ClassPathXmlApplicationContext("org/apache/activemq/camel/spring.xml");
    }
}
