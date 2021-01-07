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
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.util.AccessLogPlugin;
import org.apache.activemq.management.TimeStatisticImpl;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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
        final int threads = 3;
        final int iterations = 17;
        final int totalNumberOfIterations = threads * iterations;

        latch = new CountDownLatch(totalNumberOfIterations);

        final ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int t = 0 ; t < threads ; t++) {
            final int currentThread = t;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        final Queue testQueue = session.createQueue("Test.Queue");
                        MessageProducer producer = session.createProducer(testQueue);

                        for (int i = 0; i < iterations; i++) {
                            Message msg = session.createTextMessage("This is a test " + currentThread + "/" + i);
                            producer.send(msg);

                        }

                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });
        }

        latch.await(5, TimeUnit.MINUTES);

        final ConcurrentMap<String, DescriptiveStatistics> stats = new ConcurrentHashMap<>();
        for (AccessLogPlugin.Timing timing : timingList) {
            for (AccessLogPlugin.Breakdown breakdown : timing.getBreakdowns()) {
                stats.computeIfAbsent(breakdown.getWhat(), (key) -> new DescriptiveStatistics());
                stats.computeIfPresent(breakdown.getWhat(), (key, value) -> {
                    value.addValue(breakdown.getTiming());
                    return value;
                });
            }
        }

        System.out.println("======");
        for (Map.Entry<String, DescriptiveStatistics> entry : stats.entrySet()) {
            System.out.printf("name = %-60s, count = %-5d, average = %-10.0f, min = %-10.0f, max = %-10.0f, 90p = %-10.0f, 95p = %-10.0f, 99p = %-10.0f \n", entry.getKey(),
                              entry.getValue().getN(), entry.getValue().getMean(),
                              entry.getValue().getMin(), entry.getValue().getMax(),
                              entry.getValue().getPercentile(90), entry.getValue().getPercentile(95),
                              entry.getValue().getPercentile(99));
        }
        System.out.println("======");

        final AccessLogPlugin plugin = (AccessLogPlugin) broker.getPlugins()[0];
        final TimeStatisticImpl[] timeStatistics = plugin.getTimeStatistics();
        displayTimings(timeStatistics);
    }

    private void displayTimings(final TimeStatisticImpl[] timeStatistics) {
        System.out.println("======");
        for (TimeStatisticImpl timeStatistic : timeStatistics) {
            System.out.printf("name = %-60s, count = %-5d, average = %-10.0f, min = %-10d, max = %-10d \n", timeStatistic.getName(),
                              timeStatistic.getCount(), timeStatistic.getAverageTime(),
                              timeStatistic.getMinTime(), timeStatistic.getMaxTime());
        }
        System.out.println("======");
    }

    private void displayStats(final String what, final DescriptiveStatistics statistics) {
        final StringBuilder sb = new StringBuilder();
        sb.append("--> Stats for ").append(what).append(" --> ").append("\n");
        sb.append("\t").append(statistics.toString());
        sb.append("90 percentile = ").append(statistics.getPercentile(90d)).append("\n");
        sb.append("95 percentile = ").append(statistics.getPercentile(95d)).append("\n");
        sb.append("99 percentile = ").append(statistics.getPercentile(99d)).append("\n");
        System.out.println(sb.toString());
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

        Assert.assertTrue(items.contains("StoreQueueTask.acquireLocks"));
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
        broker = createBroker(true);
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
        answer.getManagementContext().start();
        if (withAccessLogPlugin) {
            BrokerPlugin[] plugins = new BrokerPlugin[1];

            final AccessLogPlugin accessLogPlugin = new AccessLogPlugin();
            accessLogPlugin.setEnabled(true);
            accessLogPlugin.setThreshold(0);
            accessLogPlugin.setCallback(timing -> {
                timingList.add(timing);
                latch.countDown();
            });
            plugins[0] = accessLogPlugin;
            answer.setPlugins(plugins);
        }
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector("tcp://localhost:0");
        answer.start();

        return answer;
    }

}
