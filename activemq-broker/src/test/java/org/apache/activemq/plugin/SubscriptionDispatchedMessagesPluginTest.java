package org.apache.activemq.plugin;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.plugin.SubscriptionDispatchedMessagesPlugin;
import org.apache.activemq.selector.SelectorParser;
import org.junit.Assert;
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
import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public class SubscriptionDispatchedMessagesPluginTest {

    // Basic tests to ensure MBeans are created and cleaned up

    @Test
    public void testQueueSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", message -> { });
        Assert.assertEquals(1, getObjectNames("TEST1").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST1").size());

        broker.stop();
    }

    @Test
    public void testNonDurableTopicSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC1", message -> { });
        Assert.assertEquals(1, getObjectNames("TOPIC1").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TOPIC1").size());

        broker.stop();
    }

    @Test
    public void testDurableTopicSubscription() throws Exception {
        final BrokerService broker = createBroker();
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC2", "ClientA", message -> { });
        Assert.assertEquals(1, getObjectNames("TOPIC2").size());

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TOPIC2").size());

        broker.stop();
    }

    // Tests with additionalPredicate

    @Test
    public void testQueueSubscriptionWithAdditionalPredicate() throws Exception {
        final BrokerService broker = createBroker("domainSelector=test", "TEST1");
        final MessageConsumer messageConsumer = listenOnQueue("vm://localhost", "TEST1", message -> { });
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
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC1", message -> { });
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
        final MessageConsumer messageConsumer = listenOnTopic("vm://localhost", "TOPIC2","ClientA", message -> { });
        Assert.assertEquals(1, getObjectNames("TOPIC2").size());

        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Object attribute = mBeanServer.getAttribute(getObjectNames("TOPIC2").iterator().next(), "AdditionalPredicate");
        Assert.assertEquals("(domainSelector = test)", attribute);

        messageConsumer.close();
        Assert.assertEquals(0, getObjectNames("TEST2").size());

        broker.stop();
    }

    // Tests with unacknowledged messages

    public void testQueueSubscriptionWithUnackowledgedMessages() throws Exception {

    }

    public void testNonDurableTopicSubscriptionWithUnackowledgedMessages() throws Exception {

    }

    public void testDurableTopicSubscriptionWithUnackowledgedMessages() throws Exception {

    }

    // Some additional tests to potentially reproduce the issues with stuck messages

    public void testUnacknowledgedTestDomainMessages() throws Exception {

    }

    private Set<ObjectName> getObjectNames(String destinationName) throws MalformedObjectNameException {
        final MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        final Set<ObjectName> objectNames = mBeanServer.queryNames(new ObjectName("org.apache.activemq:*,view=extended"), null);
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

    protected void createQueueConsumer(final String brokerUrl, final String queueName) throws Exception {

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

    protected void produceMessagesOnQueue(final String brokerUrl, final String queueName, final int numberOfMessages) throws Exception {
        produceMessagesOnQueue(brokerUrl, queueName, numberOfMessages, 0);
    }

    protected void produceMessagesOnQueue(final String brokerUrl, final String queueName, final int numberOfMessages, int messageSize) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        sendMessagesToDestination(numberOfMessages, messageSize, session, queue);
        session.close();
        conn.close();
    }

    protected void produceMessagesOnTopic(final String brokerUrl, final String topicName, final int numberOfMessages) throws Exception {
        produceMessagesOnTopic(brokerUrl, topicName, numberOfMessages, 0);
    }

    private void produceMessagesOnTopic(final String brokerUrl, final String topicName, final int numberOfMessages, int messageSize) throws Exception {
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUrl);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        final Topic topic = session.createTopic(topicName);
        sendMessagesToDestination(numberOfMessages, messageSize, session, topic);
        session.close();
        conn.close();
    }

    private void sendMessagesToDestination(final int numberOfMessages, final int messageSize, final Session session, final Destination dest) throws JMSException, IOException {
        final MessageProducer producer = session.createProducer(dest);

        for (int i = 0; i < numberOfMessages; i++) {
            final String messageText;
            if (messageSize < 1) {
                messageText = "Test message: " + i;
            } else {
                messageText = readInputStream(getClass().getResourceAsStream("demo.txt"), messageSize, i);
            }

            final TextMessage message = session.createTextMessage(messageText);
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
        final ConnectionFactory cf = new ActiveMQConnectionFactory(brokerUri);
        final Connection conn = cf.createConnection();
        conn.start();

        final Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        final Queue queue = session.createQueue(queueName);
        final MessageConsumer consumer = session.createConsumer(queue);
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
        final TopicSubscriber consumer = session.createDurableSubscriber(topic, clientID);
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