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
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.lang.RandomStringUtils;
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
import java.io.File;
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

public class RequestPerformanceNoLoggingTest extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(RequestPerformanceNoLoggingTest.class);

    private ConnectionFactory factory;
    private CountDownLatch latch;
    private List<AccessLogPlugin.Timing> timingList = new ArrayList<>();


    public void testMultiThread() throws Exception {
        final int threads = 20;
        final int iterations = 1_000_000;
        final int totalNumberOfIterations = threads * iterations;

        final String randomString = RandomStringUtils.randomAlphanumeric(1024 * 1024 * 25); // 5 MB

        latch = new CountDownLatch(totalNumberOfIterations);

        final ExecutorService executorService = Executors.newFixedThreadPool(threads);
        for (int t = 0 ; t < threads ; t++) {
            final int currentThread = t;
            executorService.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        final Connection con = factory.createConnection();
                        con.start();
                        final Session session = con.createSession(true, Session.AUTO_ACKNOWLEDGE);
                        //final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        final Queue testQueue = session.createQueue("Test.Queue" + currentThread);
                        MessageProducer producer = session.createProducer(testQueue);

                        for (int i = 0; i < iterations; i++) {
                            Message msg = session.createTextMessage(randomString);
                            producer.send(msg);

                        }

                        con.close();

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
        /*
        for (AccessLogPlugin.Breakdown b : timingList.get(0).getBreakdowns()) {
            System.out.printf("name = %-60s, average = %10d \n", b.getWhat(), b.getTiming());
        }
         */
        for (Map.Entry<String, DescriptiveStatistics> entry : stats.entrySet()) {
            System.out.printf("name = %-60s, count = %-5d, average = %10.0f, min = %10.0f, max = %10.0f, 90p = %-10.0f, 95p = %-10.0f, 99p = %-10.0f \n", entry.getKey(),
                              entry.getValue().getN(), entry.getValue().getMean(),
                              entry.getValue().getMin(), entry.getValue().getMax(),
                              entry.getValue().getPercentile(90), entry.getValue().getPercentile(95),
                              entry.getValue().getPercentile(99));
        }

    }

    @Override
    public void setUp() throws Exception {
        factory = new ActiveMQConnectionFactory("tcp://localhost:61616");
    }


}
