/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.util;

import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.AsyncAnnotatedMBean;
import org.apache.activemq.broker.jmx.OpenTypeSupport;
import org.apache.activemq.command.Message;
import org.apache.activemq.management.TimeStatisticImpl;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.SimpleType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tracks and logs timings for messages being sent to a destination
 *
 * @org.apache.xbean.XBean
 */
public class AccessLogPlugin extends BrokerPluginSupport {

    private static final Logger LOG = LoggerFactory.getLogger("TIMING");
    private static final ThreadLocal<String> THREAD_MESSAGE_ID = new ThreadLocal<>();

    private final AtomicBoolean enabled = new AtomicBoolean(true);
    private final AtomicInteger threshold = new AtomicInteger(0);

    private final Timings timings = new Timings();
    private RecordingCallback recordingCallback;

    @PostConstruct
    private void postConstruct() {
        try {
            afterPropertiesSet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    public void afterPropertiesSet() throws Exception {
        LOG.info("Created AccessLogPlugin: {}", this.toString());
    }

    @Override
    public void start() throws Exception {
        super.start();

        if (getBrokerService().isUseJmx()) {
            OpenTypeSupport.addFactory(TimeStatisticImpl.class, new OpenTypeSupport.AbstractOpenTypeFactory() {
                @Override
                protected String getTypeName() {
                    return TimeStatisticImpl.class.getName();
                }

                @Override
                protected void init() throws OpenDataException {
                    super.init();
                    addItem("name", "name", SimpleType.STRING);
                    addItem("count", "count", SimpleType.LONG);
                    addItem("minTime", "minTime", SimpleType.LONG);
                    addItem("maxTime", "maxTime", SimpleType.LONG);
                    addItem("totalTime", "totalTime", SimpleType.LONG);
                    addItem("averageTime", "averageTime", SimpleType.DOUBLE);
                    // addItem("averagePerSecond", "averagePerSecond", SimpleType.DOUBLE);
                    // addItem("averagePerSecondExcludingMinMax", "averagePerSecondExcludingMinMax", SimpleType.DOUBLE);
                    addItem("averageTimeExcludingMinMax", "averageTimeExcludingMinMax", SimpleType.DOUBLE);
                    addItem("lastSampleTime", "lastSampleTime", SimpleType.LONG);
                }

                @Override
                public Map<String, Object> getFields(Object o) throws OpenDataException {
                    TimeStatisticImpl statistic = (TimeStatisticImpl) o;
                    Map<String, Object> rc = super.getFields(o);
                    rc.put("name", statistic.getName());
                    rc.put("count", statistic.getCount());
                    rc.put("minTime", statistic.getMinTime());
                    rc.put("maxTime", statistic.getMaxTime());
                    rc.put("totalTime", statistic.getTotalTime());
                    rc.put("averageTime", statistic.getAverageTime());
                    // rc.put("averagePerSecond", statistic.getAveragePerSecond());
                    // rc.put("averagePerSecondExcludingMinMax", statistic.getAveragePerSecondExcludingMinMax());
                    rc.put("averageTimeExcludingMinMax", statistic.getAverageTimeExcludingMinMax());
                    rc.put("lastSampleTime", statistic.getLastSampleTime());
                    return rc;
                }
            });
            AsyncAnnotatedMBean.registerMBean(
                this.getBrokerService().getManagementContext(),
                new AccessLogView(this),
                createJmxName(getBrokerService().getBrokerObjectName().toString(), "AccessLogPlugin")
                                             );
        }
    }

    @Override
    public void stop() throws Exception {
        if (getBrokerService().isUseJmx()) {
            final ObjectName name =
                createJmxName(getBrokerService().getBrokerObjectName().toString(), "AccessLogPlugin");
            getBrokerService().getManagementContext().unregisterMBean(name);
        }

        dumpStats();

        super.stop();
    }

    public static ObjectName createJmxName(final String brokerObjectName, final String name)
        throws MalformedObjectNameException {
        String objectNameStr = brokerObjectName;

        objectNameStr += "," + "service=AccessLog";
        objectNameStr += "," + "instanceName=" + JMXSupport.encodeObjectNamePart(name);

        return new ObjectName(objectNameStr);
    }

    public boolean isEnabled() {
        return enabled.get();
    }

    public void setEnabled(final boolean enabled) {
        this.enabled.set(enabled);
    }

    public int getThreshold() {
        return threshold.get();
    }

    public void setThreshold(final int threshold) {
        this.threshold.set(threshold);
    }

    public void resetTimeStatistics() {
        timings.resetStatistics();
    }

    public TimeStatisticImpl[] getTimeStatistics() {
        return timings.getStatistics();
    }

    @Override
    public void send(final ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        if (!enabled.get()) {
            super.send(producerExchange, messageSend);
            return;
        }

        THREAD_MESSAGE_ID.set(messageSend.getMessageId().toString());
        long start = System.nanoTime();

        try {
            if (enabled.get()) {
                timings.start(messageSend);
            }
            super.send(producerExchange, messageSend);
        } finally {
            if (enabled.get()) {
                timings.end(messageSend, start);
            }

            THREAD_MESSAGE_ID.set(null);
        }
    }

    public void record(final String messageId, final String what, final long duration) {
        if (!enabled.get()) {
            return;
        }

        String id = messageId;
        if (id == null) {
            id = THREAD_MESSAGE_ID.get();
        }

        if (id == null) {
            LOG.info(String.format("Discarding timing breakdown without messageId (%40s=%10d)", what, duration));
            // return;
        }

        timings.record(id, what, duration);
    }

    public void setThreadMessageId(final String messageId) {
        THREAD_MESSAGE_ID.set(messageId);
    }

    public void setCallback(RecordingCallback recordingCallback) {
        this.recordingCallback = recordingCallback;
    }

    private class Timings {
        private ConcurrentMap<String, Timing> inflight = new ConcurrentHashMap<>();
        private ConcurrentMap<String, TimeStatisticImpl> timeStatistics = new ConcurrentHashMap<>();

        public void start(final Message message) {
            final String messageId = message.getMessageId().toString();
            final int messageSize = message.getContent() != null ? message.getContent().getLength() : -1;

            inflight.computeIfAbsent(messageId, (key) -> {
                final String destination = message.getDestination() != null ? message.getDestination().toString() : "";
                return new Timing(key, destination, messageSize);
            });
        }

        public void end(final Message message, final long start) {
            final long duration = System.nanoTime() - start;
            final String messageId = message.getMessageId().toString();

            record(messageId, "whole_request", duration);

            final Timing timing = inflight.remove(messageId);

            final int th = threshold.get();
            if (th <= 0 || ((long) th < (duration / 1000000))) {

                if (false && LOG.isInfoEnabled()) {
                    LOG.info(timing.toString());
                }
                if (recordingCallback != null) {
                    recordingCallback.sendComplete(timing);
                }
            }
        }

        public void record(final String messageId, final String what, final long duration) {

            /*
            if (false && Arrays.asList("MessageDatabase.journal_write", "MessageDatabase.index_write").contains(what)) {
                new Throwable().printStackTrace();
            }
            */
            if (messageId != null) {
                inflight.computeIfPresent(messageId, (key, timing) -> timing.add(what, duration));
            }
            timeStatistics.computeIfAbsent(what, ((key) -> new TimeStatisticImpl(key, "ns", key)));
            timeStatistics.computeIfPresent(what, (key, timing) -> {
                timing.addTime(duration);
                return timing;
            });
        }

        public TimeStatisticImpl[] getStatistics() {
            return timeStatistics.values().toArray(new TimeStatisticImpl[0]);
        }

        public void resetStatistics() {
            timeStatistics.forEach((key, timing) -> timing.reset());
        }
    }

    public class Timing {
        private final String messageId;
        private final String destination;
        private final int messageSize;
        private final List<Breakdown> timingBreakdowns = Collections.synchronizedList(new ArrayList<>());

        private Timing(final String messageId, final String destination, final int messageSize) {
            this.messageId = messageId;
            this.destination = destination;
            this.messageSize = messageSize;
        }

        public Timing add(final String what, final long duration) {
            timingBreakdowns.add(new Breakdown(what, duration));
            return this;
        }

        @Override
        public String toString() {
            return "Timing{" +
                   "messageId='" + messageId + '\'' +
                   ", destination='" + destination + '\'' +
                   ", messageSize='" + messageSize + '\'' +
                   ", timingBreakdowns=" + timingBreakdowns +
                   '}';
        }

        public List<Breakdown> getBreakdowns() {
            return timingBreakdowns;
        }
    }

    public static class Breakdown {
        private final String what;
        private final Long timing;

        public Breakdown(final String what, final Long timing) {
            this.what = what;
            this.timing = timing;
        }

        public String getWhat() {
            return what;
        }

        public Long getTiming() {
            return timing;
        }

        @Override
        public String toString() {
            return "Breakdown{" +
                   "what='" + getWhat() + '\'' +
                   ", timing=" + getTiming() +
                   '}';
        }
    }

    private void dumpStats() {
        final TimeStatisticImpl[] timeStatistics = getTimeStatistics();
        displayTimings(timeStatistics);

    }

    private void displayTimings(final TimeStatisticImpl[] timeStatistics) {
        final StringBuilder sb = new StringBuilder();
        sb.append("\n\n======");
        double wholeRequestTps = 0;
        for (TimeStatisticImpl timeStatistic : timeStatistics) {
            sb.append(String.format("name = %-60s, count = %-5d, average = %10.0f, min = %10d, max = %10d \n", timeStatistic.getName(),
                              timeStatistic.getCount(), timeStatistic.getAverageTime(),
                              timeStatistic.getMinTime(), timeStatistic.getMaxTime()));

            if ("whole_request".equals(timeStatistic.getName())) {
                wholeRequestTps = timeStatistic.getCount() / ((double) timeStatistic.getTotalTime() / (double) 1_000_000_000);
            }
        }

        sb.append(String.format("\n>> Whole Request TPS = %10.0f \n\n", wholeRequestTps));
        LOG.info(sb.toString());
    }

    public interface RecordingCallback {
        void sendComplete(final Timing timing);
    }
}
