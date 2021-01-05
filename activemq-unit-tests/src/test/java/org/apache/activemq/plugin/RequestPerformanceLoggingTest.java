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
package org.apache.activemq.plugin;

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.util.AccessLogPlugin;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class RequestPerformanceLoggingTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(RequestPerformanceLoggingTest.class);

    private Connection connection;
    private BrokerService broker;
    private CountDownLatch latch;
    private List<AccessLogPlugin.Timing> timingList = new ArrayList<>();

    public void testMultiThread() throws Exception {
        final int threads = 40;
        final int iterations = 15000;
        final int totalNumberOfIterations = threads * iterations;

        latch = new CountDownLatch(totalNumberOfIterations);
        final DescriptiveStatistics descriptiveStatistics = new DescriptiveStatistics();

        final ExecutorService executorService = Executors.newFixedThreadPool(20);
        for (int t = 0 ; t < threads ; t++) {
            final int currentThread = t;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        Queue replyTo = session.createTemporaryQueue();
                        MessageConsumer consumer = session.createConsumer(replyTo);
                        Queue query = session.createQueue(StatisticsBroker.STATS_BROKER_PREFIX);
                        MessageProducer producer = session.createProducer(query);
                        for (int i = 0; i < iterations; i++) {
                            final long start = System.nanoTime();
                            Message msg = session.createMessage();
                            msg.setJMSReplyTo(replyTo);
                            producer.send(msg);

                            MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
                            assertNotNull(reply);
                            assertTrue(reply.getMapNames().hasMoreElements());
                            assertTrue(reply.getJMSTimestamp() > 0);
                            assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());

                            latch.countDown();
                            descriptiveStatistics.addValue(System.nanoTime() - start);
                        }

                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await(1, TimeUnit.MINUTES);

        long total = 0;
        for (AccessLogPlugin.Timing timing : timingList) {
            for (AccessLogPlugin.Breakdown breakdown : timing.getBreakdowns()) {
                if ("whole_request".equals(breakdown.getWhat())) {
                    total += breakdown.getTiming();
                    break;
                }
            }
        }

        System.out.println("Average for whole request = " + total / totalNumberOfIterations);

        System.out.println("Stats for whole request = " + descriptiveStatistics.toString());
        System.out.println("90 percentile for whole request = " + descriptiveStatistics.getPercentile(90d));
        System.out.println("95 percentile for whole request = " + descriptiveStatistics.getPercentile(95d));
        System.out.println("99 percentile for whole request = " + descriptiveStatistics.getPercentile(99d));

    }

    public void testDestinationStats() throws Exception {
        latch = new CountDownLatch(1);

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(testQueue);
        Message msg = session.createTextMessage("This is a test");
        producer.send(msg);

        latch.await(1, TimeUnit.MINUTES);
        Assert.assertEquals(1, timingList.size());

        final Set<String> items = new HashSet<>();
        List<AccessLogPlugin.Breakdown> breakdownList = timingList.get(0).getBreakdowns();
        for (final AccessLogPlugin.Breakdown breakdown : breakdownList) {
            items.add(breakdown.getWhat());
            System.out.println(breakdown.getWhat() + "=" + breakdown.getTiming());
        }

        Assert.assertTrue(items.contains("StoreQueueTask.aquireLocks"));
        Assert.assertTrue(items.contains("MessageDatabase.store:checkpointLock.readLock().lock()"));
        Assert.assertTrue(items.contains("DataFileAppender.writeBatch"));
        Assert.assertTrue(items.contains("DataFileAppender.rollover"));
        Assert.assertTrue(items.contains("MessageDatabase.journal_write"));
        Assert.assertTrue(items.contains("MessageDatabase.index_write"));
        Assert.assertTrue(items.contains("StoreQueueTask.store.addMessage"));
        Assert.assertTrue(items.contains("StoreQueueTask.run"));
        Assert.assertTrue(items.contains("whole_request"));
    }

    @Override
    protected void setUp() throws Exception {
        broker = createBroker(false);
        ConnectionFactory factory = new ActiveMQConnectionFactory(broker.getTransportConnectorURIsAsMap().get("tcp"));
        connection = factory.createConnection();
        connection.start();
    }

    @Override
    protected void tearDown() throws Exception{
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.broker!=null) {
            this.broker.stop();
        }
    }

    protected BrokerService createBroker(final boolean withAccessLogPlugin) throws Exception {
        BrokerService answer = new BrokerService();
        BrokerPlugin[] plugins;
        if (withAccessLogPlugin) {
            plugins = new BrokerPlugin[2];
            plugins[0] = new StatisticsBrokerPlugin();

            final AccessLogPlugin accessLogPlugin = new AccessLogPlugin();
            accessLogPlugin.setEnabled(true);
            accessLogPlugin.setThreshold(0);
            accessLogPlugin.setCallback(new AccessLogPlugin.RecordingCallback() {

                @Override
                public void sendComplete(AccessLogPlugin.Timing timing) {
                    timingList.add(timing);
                }
            });
            plugins[1] = accessLogPlugin;

        } else {
            plugins = new BrokerPlugin[1];
            plugins[0] = new StatisticsBrokerPlugin();
        }
        answer.setPlugins(plugins);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector("tcp://localhost:0");
        answer.start();
        return answer;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
