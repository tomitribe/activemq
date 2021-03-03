package org.apache.activemq.plugin;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.DestinationInterceptor;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.selector.SelectorParser;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

public class SubscriptionDispatchedMessagesPluginTest {

    // Basic tests to ensure MBeans are created and cleaned up

    @Test
    public void testQueueSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TEST1").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    public void testNonDurableTopicSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC1", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TOPIC1").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TOPIC1").size());

        broker.stop();
    }

    @Test
    public void testDurableTopicSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC2", "ClientA", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TOPIC2").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TOPIC2").size());

        broker.stop();
    }

    // Tests with additionalPredicate

    @Test
    public void testQueueSubscriptionWithAdditionalPredicate() throws Exception {
        final BrokerService broker = createBroker("domainSelector=test", "TEST1");
        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TEST1").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Object attribute = mBeanServer.getAttribute(getObjectNames("TEST1").iterator().next(), "AdditionalPredicate");
        Assert.assertEquals("(domainSelector = test)", attribute);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    public void testNonDurableTopicSubscriptionWithAdditionalPredicate() throws Exception {
        final BrokerService broker = createBroker("domainSelector=test", "TOPIC1");
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC1", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TOPIC1").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Object attribute = mBeanServer.getAttribute(getObjectNames("TOPIC1").iterator().next(), "AdditionalPredicate");
        Assert.assertEquals("(domainSelector = test)", attribute);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    public void testDurableTopicSubscriptionWithAdditionalPredicate() throws Exception {
        final BrokerService broker = createBroker("domainSelector=test", "TOPIC2");
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC2","ClientA", this::acknowledge);
        Assert.assertEquals(1, getObjectNames("TOPIC2").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Object attribute = mBeanServer.getAttribute(getObjectNames("TOPIC2").iterator().next(), "AdditionalPredicate");
        Assert.assertEquals("(domainSelector = test)", attribute);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST2").size());

        broker.stop();
    }

    // Tests with unacknowledged messages

    @Test
    public void testQueueSubscriptionWithUnackowledgedMessages() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", this::consumeAndDontAcknowledge);

        produceMessagesOnQueue("vm://localhost", "TEST1", 100, null);
        Assert.assertEquals(1, getObjectNames("TEST1").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final String[] attribute = (String[]) mBeanServer.getAttribute(getObjectNames("TEST1").iterator().next(), "UnackedMessageIds");
        Assert.assertEquals(100, attribute.length);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    public void testNonDurableTopicSubscriptionWithUnackowledgedMessages() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC1", this::consumeAndDontAcknowledge);

        produceMessagesOnTopic("vm://localhost", "TOPIC1", 100, null);
        Assert.assertEquals(1, getObjectNames("TOPIC1").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final String[] attribute = (String[]) mBeanServer.getAttribute(getObjectNames("TOPIC1").iterator().next(), "UnackedMessageIds");
        Assert.assertEquals(1, attribute.length);
        Assert.assertEquals("Not a pre-fetch subscription.", attribute[0]);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    @Ignore
    public void testDurableTopicSubscriptionWithUnackowledgedMessages() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC2","ClientA", this::consumeAndDontAcknowledge);

        produceMessagesOnTopic("vm://localhost", "TOPIC2", 100, null);
        Assert.assertEquals(1, getObjectNames("TOPIC2").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final String[] attribute = (String[]) mBeanServer.getAttribute(getObjectNames("TOPIC2").iterator().next(), "UnackedMessageIds");
        Assert.assertEquals(100, attribute.length);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TOPIC2").size());

        broker.stop();
    }

    // Some additional tests to potentially reproduce the issues with stuck messages
    @Test
    public void testNotDispatchedTestDomainMessages() throws Exception {
        final BrokerService broker = createBroker("domainSelector='test'", "TEST1");

        final AtomicInteger consumed = new AtomicInteger(0);

        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", message -> {
            try {
                message.acknowledge();
                consumed.incrementAndGet();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        Map<String, String> testDomainMessageProperties = new HashMap<>();
        testDomainMessageProperties.put("domainSelector", "test");

        Map<String, String> clientDomainMessageProperties = new HashMap<>();
        clientDomainMessageProperties.put("domainSelector", "client");

        // these at the head of the queue will match the additional predicate and should be consumed
        produceMessagesOnQueue("vm://localhost", "TEST1", 2000, testDomainMessageProperties);

        // these do not match the additional predicate and will not be consumed, they remain in the queue
        produceMessagesOnQueue("vm://localhost", "TEST1", 199, clientDomainMessageProperties);

        // these *do* match the predicate, and should not be stuck behind the 199 non-matching messages
        produceMessagesOnQueue("vm://localhost", "TEST1", 5000, testDomainMessageProperties);

        // we're expecting 7k messages to be dispatched and ACK'd
        Assert.assertEquals(7000, consumed.get());

        messageConsumer.close();

        broker.stop();
    }

    @Test
    public void testNotAckedTestDomainMessages() throws Exception {
        final BrokerService broker = createBroker();

        final AtomicInteger consumed1 = new AtomicInteger(0);
        final AtomicInteger consumed2 = new AtomicInteger(0);

        final MessageConsumer messageConsumer1 = listenOnQueue("vm://localhost", "TEST1", "domainSelector = 'test'",m -> {
            consumed1.incrementAndGet();
        });

        final MessageConsumer messageConsumer2 = listenOnQueue("vm://localhost", "TEST1", "domainSelector = 'service'",m -> {
            consumed2.incrementAndGet();
            try {
                m.acknowledge();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        Map<String, String> testDomainMessageProperties = new HashMap<>();
        testDomainMessageProperties.put("domainSelector", "test");

        Map<String, String> serviceDomainMessageProperties = new HashMap<>();
        serviceDomainMessageProperties.put("domainSelector", "service");

        // these at the head of the queue will match the additional predicate and should be consumed
        produceMessagesOnQueue("vm://localhost", "TEST1", 10000, testDomainMessageProperties);

        // these at the head of the queue will match the additional predicate and should be consumed
        produceMessagesOnQueue("vm://localhost", "TEST1", 10000, serviceDomainMessageProperties);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Iterator<ObjectName> iterator = getObjectNames("TEST1").iterator();
        int unackedMessages = 0;
        while (iterator.hasNext()) {
            final ObjectName objectName = iterator.next();
            final String[] attribute = (String[]) mBeanServer.getAttribute(objectName, "UnackedMessageIds");
            unackedMessages += attribute.length;
        }

        Assert.assertEquals(10000, unackedMessages);

        Assert.assertEquals(10000, consumed1.get());
        Assert.assertEquals(10000, consumed2.get());


        messageConsumer1.close();
        messageConsumer2.close();
        broker.stop();
    }

    @Test
    public void testSelectorAwareDomainMessages() throws Exception {
        final BrokerService broker = createBroker("domainSelector='test'", "Consumer.A.VirtualTopic.TEST1");
        turnOnSelectorAwareVirtualTopics(broker);

        final AtomicInteger consumed = new AtomicInteger(0);

        final MessageConsumer messageConsumer1 = listenOnQueue("vm://localhost", "Consumer.A.VirtualTopic.TEST1" ,m -> {
            consumed.incrementAndGet();
            try {
                m.acknowledge();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        });

        Map<String, String> testDomainMessageProperties = new HashMap<>();
        testDomainMessageProperties.put("domainSelector", "test");

        Map<String, String> serviceDomainMessageProperties = new HashMap<>();
        serviceDomainMessageProperties.put("domainSelector", "service");

        // these at the head of the queue will match the additional predicate and should be consumed
        produceMessagesOnTopic("vm://localhost", "VirtualTopic.TEST1", 10000, testDomainMessageProperties);
        produceMessagesOnTopic("vm://localhost", "VirtualTopic.TEST1", 10000, serviceDomainMessageProperties);
        produceMessagesOnTopic("vm://localhost", "VirtualTopic.TEST1", 10000, testDomainMessageProperties);

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Iterator<ObjectName> iterator = getObjectNames("Consumer.A.VirtualTopic.TEST1").iterator();
        int unackedMessages = 0;
        while (iterator.hasNext()) {
            final ObjectName objectName = iterator.next();
            final String[] attribute = (String[]) mBeanServer.getAttribute(objectName, "UnackedMessageIds");
            unackedMessages += attribute.length;
        }

        Assert.assertEquals(0, unackedMessages);
        Assert.assertEquals(20000, consumed.get());

        final org.apache.activemq.broker.region.Destination destination = broker.getRegionBroker().getDestinationMap().get(new ActiveMQQueue("Consumer.A.VirtualTopic.TEST1"));
        Assert.assertEquals(0, getPendingMessageCount(destination));

        messageConsumer1.close();
        broker.stop();
    }

    private long getPendingMessageCount(org.apache.activemq.broker.region.Destination destination) {
        if (destination instanceof org.apache.activemq.broker.region.Queue) {
            return ((org.apache.activemq.broker.region.Queue) destination).getPendingMessageCount();
        }

        if (destination instanceof DestinationFilter) {
            try {
                final Field nextField = DestinationFilter.class.getDeclaredField("next");
                nextField.setAccessible(true);
                final Object o = nextField.get(destination);
                if (o instanceof org.apache.activemq.broker.region.Destination) {
                    return getPendingMessageCount((org.apache.activemq.broker.region.Destination) o);
                }
            } catch (NoSuchFieldException | IllegalAccessException e) {
                throw new IllegalArgumentException("Unable to get pending message count from " + destination.getClass().getCanonicalName());
            }
        }

        throw new IllegalArgumentException("Unable to get pending message count from " + destination.getClass().getCanonicalName());
    }

    private void turnOnSelectorAwareVirtualTopics(BrokerService broker) {
        final DestinationInterceptor[] destinationInterceptors = broker.getDestinationInterceptors();
        for (final DestinationInterceptor destinationInterceptor : destinationInterceptors) {
            if (destinationInterceptor instanceof VirtualDestinationInterceptor) {
                final VirtualDestinationInterceptor virtualDestinationInterceptor = (VirtualDestinationInterceptor) destinationInterceptor;
                final VirtualDestination[] virtualDestinations = virtualDestinationInterceptor.getVirtualDestinations();
                for (final VirtualDestination virtualDestination : virtualDestinations) {
                    if (virtualDestination instanceof VirtualTopic) {
                        ((VirtualTopic)virtualDestination).setSelectorAware(true);
                    }
                }
            }
        }
    }

    private void acknowledge(final Message message) {
        try {
            message.acknowledge();
        } catch (JMSException e) {
            throw new RuntimeException(e);
        }
    }

    private void consumeAndDontAcknowledge(final Message message) {
        // no-op
    }

    private Set<ObjectName> getObjectNames(String destinationName) throws MalformedObjectNameException {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Set<ObjectName> objectNames = mBeanServer.queryNames(new ObjectName("com.tomitribe.activemq:*,view=extended"), null);
        objectNames.removeIf(o -> (! o.toString().contains("destinationName=" + destinationName)));
        return objectNames;
    }

    protected void cancelSubscription(final String brokerUrl, final String clientName) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.setClientID(clientName);
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe(clientName);
        session.close();
        conn.stop();
        conn.close();
    }

    protected MessageConsumer createDurableSubscriber(final String brokerUrl, final String topicName, final String clientName) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.setClientID(clientName);
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Topic topic = session.createTopic(topicName);
        final TopicSubscriber subscriber = session.createDurableSubscriber(topic, clientName);

        return new MessageConsumerWrapper(subscriber, session, conn);
    }

    protected void consumeMessagesFromQueue(final String brokerUrl, final String queueName, final int numberOfMessages) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageConsumer consumer = session.createConsumer(queue);

        for (int i = 0; i < numberOfMessages; i++) {
            final Message receivedMessage = consumer.receive(1000);
            Assert.assertNotNull(receivedMessage);
        }

        consumer.close();
        session.close();
        conn.close();
    }

    protected Message getNextMessageFromQueue(final String brokerUrl, final String queueName) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageConsumer consumer = session.createConsumer(queue);

        final Message receivedMessage = consumer.receive(1000);

        consumer.close();
        session.close();
        conn.close();

        return receivedMessage;
    }

    protected void produceMessagesOnQueue(final String brokerUrl, final String queueName, final int numberOfMessages, final Map<String, String> messageProperties) throws Exception {
        produceMessagesOnQueue(brokerUrl, queueName, numberOfMessages, 0, messageProperties);
    }

    protected void produceMessagesOnQueue(final String brokerUrl, final String queueName, final int numberOfMessages, int messageSize, final Map<String, String> messageProperties) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        sendMessagesToDestination(numberOfMessages, messageSize, session, queue, messageProperties);
        session.close();
        conn.close();
    }

    protected void produceMessagesOnTopic(final String brokerUrl, final String topicName, final int numberOfMessages, final Map<String, String> messageProperties) throws Exception {
        produceMessagesOnTopic(brokerUrl, topicName, numberOfMessages, 0, messageProperties);
    }

    private void produceMessagesOnTopic(final String brokerUrl, final String topicName, final int numberOfMessages, int messageSize, final Map<String, String> messageProperties) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Topic topic = session.createTopic(topicName);
        sendMessagesToDestination(numberOfMessages, messageSize, session, topic, messageProperties);
        session.close();
        conn.close();
    }

    private void sendMessagesToDestination(final int numberOfMessages, final int messageSize, final Session session, final Destination dest, final Map<String, String> messageProperties) throws JMSException, IOException {
        final MessageProducer producer = session.createProducer(dest);

        for (int i = 0; i < numberOfMessages; i++) {
            final String messageText;
            if (messageSize < 1) {
                messageText = "Test message: " + i;
            } else {
                messageText = readInputStream(getClass().getResourceAsStream("demo.txt"), messageSize, i);
            }

            final TextMessage message = session.createTextMessage(messageText);

            if (messageProperties != null) {
                messageProperties.forEach((k, v) -> {
                    try {
                        message.setStringProperty(k, v);
                    } catch (JMSException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            message.setIntProperty("messageno", i);
            producer.send(message);
        }

        producer.close();
    }

    private String readInputStream(InputStream is, int size, int messageNumber) throws IOException {
        InputStreamReader reader = new InputStreamReader(is);
        try {
            char[] buffer;
            if (size > 0) {
                buffer = new char[size];
            } else {
                buffer = new char[1024];
            }
            int count;
            StringBuilder builder = new StringBuilder();
            while ((count = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, count);
                if (size > 0) break;
            }
            return builder.toString();
        } finally {
            reader.close();
        }
    }

    protected void stopBroker(BrokerService broker) throws Exception {
        broker.stop();
    }

    protected BrokerService createBroker() throws Exception {
        return createBroker(null);
    }

    protected BrokerService createBroker(final String additionalPredicate, final String... destinations) throws Exception {
        final BrokerService broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);

        final SubscriptionDispatchedMessagesPlugin subscriptionDispatchedMessagesPlugin = new SubscriptionDispatchedMessagesPlugin();
        if (additionalPredicate != null) {
            final AdditionalPredicateBrokerPlugin additionalPredicateBrokerPlugin = new AdditionalPredicateBrokerPlugin(additionalPredicate, Arrays.asList(destinations));
            broker.setPlugins(new BrokerPlugin[] { subscriptionDispatchedMessagesPlugin, additionalPredicateBrokerPlugin });
        } else {
            broker.setPlugins(new BrokerPlugin[] { new SubscriptionDispatchedMessagesPlugin() });
        }

        broker.start();
        return broker;
    }

    protected MessageConsumer listenOnQueue(final String brokerUri, final String queueName, final MessageListener messageListener) throws Exception {
        return listenOnQueue(brokerUri, queueName, null, messageListener);
    }

    protected MessageConsumer listenOnQueue(final String brokerUri, final String queueName, final String selector, final MessageListener messageListener) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageConsumer consumer = session.createConsumer(queue, selector);
        consumer.setMessageListener(messageListener);

        return new MessageConsumerWrapper(consumer, session, conn);
    }

    protected MessageConsumer listenOnTopic(final String brokerUri, final String topicName, final MessageListener messageListener) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        final Topic topic = session.createTopic(topicName);
        final MessageConsumer consumer = session.createConsumer(topic);
        consumer.setMessageListener(messageListener);

        return new MessageConsumerWrapper(consumer, session, conn);
    }

    protected MessageConsumer listenOnTopic(final String brokerUri, final String topicName, final String clientID, final MessageListener messageListener) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
        final Connection conn = cf.createConnection();
        conn.setClientID(clientID);
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Topic topic = session.createTopic(topicName);
        final TopicSubscriber consumer = session.createDurableSubscriber(topic, clientID + "_" + topicName +"_subscription");
        consumer.setMessageListener(messageListener);

        return new MessageConsumerWrapper(consumer, session, conn);
    }

    private static class MessageConsumerWrapper implements MessageConsumer {
        private final MessageConsumer messageConsumer;
        private final Session session;
        private final Connection conn;

        public MessageConsumerWrapper(final MessageConsumer messageConsumer, final Session session, final Connection conn) {
            this.messageConsumer = messageConsumer;
            this.session = session;
            this.conn = conn;
        }

        @Override
        public String getMessageSelector() throws JMSException {
            return messageConsumer.getMessageSelector();
        }

        @Override
        public MessageListener getMessageListener() throws JMSException {
            return messageConsumer.getMessageListener();
        }

        @Override
        public void setMessageListener(final MessageListener messageListener) throws JMSException {
            messageConsumer.setMessageListener(messageListener);
        }

        @Override
        public Message receive() throws JMSException {
            return messageConsumer.receive();
        }

        @Override
        public Message receive(final long timeout) throws JMSException {
            return messageConsumer.receive(timeout);
        }

        @Override
        public Message receiveNoWait() throws JMSException {
            return messageConsumer.receiveNoWait();
        }

        @Override
        public void close() throws JMSException {
            messageConsumer.close();
            session.close();
            conn.stop();
            conn.close();
        }
    }

    private static class AdditionalPredicateBrokerPlugin implements BrokerPlugin {

        private final String additionalPredicate;
        private final Collection<String> destinationNames;

        private AdditionalPredicateBrokerPlugin(final String additionalPredicate, final Collection<String> destinationNames) {
            this.additionalPredicate = additionalPredicate;
            this.destinationNames = destinationNames;
        }

        @Override
        public Broker installPlugin(final Broker broker) throws Exception {
            return new AdditionalPredicateBrokerFilter(broker, additionalPredicate, destinationNames);
        }
    }

    private static class AdditionalPredicateBrokerFilter extends BrokerFilter {
        private final String additionalPredicate;
        private final Collection<String> destinationNames;

        public AdditionalPredicateBrokerFilter(final Broker next, final String additionalPredicate, final Collection<String> destinationNames) {
            super(next);
            this.additionalPredicate = additionalPredicate;
            this.destinationNames = destinationNames;
        }

        @Override
        public Subscription addConsumer(final ConnectionContext context, final ConsumerInfo info) throws Exception {
            if (destinationNames.contains(info.getDestination().getPhysicalName())) {
                info.setAdditionalPredicate(SelectorParser.parse(additionalPredicate));
            }

            return super.addConsumer(context, info);
        }
    }
}