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
import org.apache.activemq.broker.BrokerStoppedException;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.jmx.AsyncAnnotatedMBean;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.JMXSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
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

    private static AccessLogPlugin instance;

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

        instance = this;

        if (getBrokerService().isUseJmx()) {
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

        super.stop();
    }

    public static void startRecord(final String messageId, final Class<?> cls, final String method) {
        try {
            if (instance != null) {
                instance.startRecord(messageId, cls.getSimpleName() + "." + method);
            }
        } catch (final BrokerStoppedException e) {
            // ignore this so we aren't dumping errors
        } catch (final Exception e) {
            LOG.error("Unable to record timing for " + cls.getSimpleName() + "." + method + ".", e);
        }
    }

    public static void stopRecord(final String messageId, final Map<String, String> data) {
        try {
            if (instance != null) {
                instance.record(messageId, data);
            }
        } catch (final BrokerStoppedException e) {
            // ignore this so we aren't dumping errors
        } catch (final Exception e) {
            LOG.error("Unable to record timing.", e);
        }
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

    private void startRecord(final String messageId, final String what) {
        if (!enabled.get()) {
            return;
        }

        String id = messageId;
        if (id == null) {
            id = THREAD_MESSAGE_ID.get();
        }

        if (id == null) {
            id = "async";
        }

        timings.startRecord(id, what);
    }

    private void record(final String messageId) {
        record(messageId, null);
    }

    private void record(final String messageId, final Map<String, String> data) {
        if (!enabled.get()) {
            return;
        }

        String id = messageId;
        if (id == null) {
            id = THREAD_MESSAGE_ID.get();
        }

        if (id == null) {
            id = "async";
        }

        if (data == null) {
            timings.stopRecord(id);

        } else {
            timings.stopRecord(id, data);
        }
    }

    public void setThreadMessageId(final String messageId) {
        THREAD_MESSAGE_ID.set(messageId);
    }

    public void setCallback(RecordingCallback recordingCallback) {
        this.recordingCallback = recordingCallback;
    }

    private class Timings {
        private ConcurrentMap<String, Timing> inflight = new ConcurrentHashMap<>();

        public void start(final Message message) {
            final String messageId = message.getMessageId().toString();
            final int messageSize = message.getContent() != null ? message.getContent().getLength() : -1;

            inflight.computeIfAbsent(messageId, (key) -> {
                final String destination = message.getDestination() != null ? message.getDestination().toString() : "";
                return new Timing(key, destination, messageSize);
            });

            // this is so we can catch the async timings not belonging to a message being processed in the current thread
            inflight.computeIfAbsent("async", (key) -> {
                return new Timing(key, null, 0);
            });

            startRecord(messageId, "whole_request");
        }

        public void end(final Message message, final long start) {
            final long duration = System.nanoTime() - start;
            final String messageId = message.getMessageId().toString();

            stopRecord(messageId);

            final Timing timing = inflight.remove(messageId);

            final int th = threshold.get();
            if (th <= 0 || ((long) th < (duration / 1_000_000))) {

                if (LOG.isInfoEnabled()) {
                    LOG.info(timing.toString());
                }
                if (recordingCallback != null) {
                    recordingCallback.sendComplete(timing);
                }
            }

            // also flush out the
            final Timing async = inflight.remove("async");
            if (async != null && async.getBreakdowns() != null && async.getBreakdowns().size() > 0) {
                if (LOG.isInfoEnabled()) {
                    LOG.info(async.toString());
                }
            }
        }

        public void startRecord(final String messageId, final String what) {
            if (messageId != null) {
                inflight.computeIfPresent(messageId, (key, timing) -> timing.start(what));
            }
        }

        public void stopRecord(final String messageId) {
            if (messageId != null) {
                inflight.computeIfPresent(messageId, (key, timing) -> timing.stop());
            }
        }

        public void stopRecord(final String messageId, final Map<String, String> data) {
            if (messageId != null) {
                inflight.computeIfPresent(messageId, (key, timing) -> timing.stop(data));
            }
        }

    }

    public class Timing {
        private final String messageId;
        private final String destination;
        private final int messageSize;
        private final List<Breakdown> timingBreakdowns = Collections.synchronizedList(new ArrayList<>());
        private final Deque<Breakdown> records = new ConcurrentLinkedDeque<>();

        // no concurrency so no need for an atomic reference
        private Long wholeRequest = 0L;

        private Timing(final String messageId, final String destination, final int messageSize) {
            this.messageId = messageId;
            this.destination = destination;
            this.messageSize = messageSize;
        }

        public Timing add(final String what, final long duration) {
            timingBreakdowns.add(new Breakdown(what, duration, 0));
            return this;
        }

        public Timing start(final String what) {
            records.push(new Breakdown(what, System.nanoTime(), records.size()));
            return this;
        }

        public Timing stop() {
            return stop(Collections.EMPTY_MAP);
        }

        public Timing stop(final Map<String, String> data) {
            final Breakdown lastRecord;
            try {
                lastRecord = records.pop();
                if (lastRecord != null) {
                    timingBreakdowns.add(new Breakdown(lastRecord.getWhat(), System.nanoTime() - lastRecord.getTiming(), lastRecord.getLevel(), data));

                    // store the whole request breakdown
                    if ("whole_request".equals(lastRecord.getWhat())) {
                        this.wholeRequest = (System.nanoTime() - lastRecord.getTiming()) / 1_000_000;
                    }
                }
            } catch (final NoSuchElementException e) {
                // ignore - for async another thread may have already picked it up
            }
            return this;
        }

        public void checkMissingStop() {
            final long stop = System.nanoTime();
            final Iterator<Breakdown> iterator = records.descendingIterator();
            while (iterator.hasNext()) {
                final Breakdown breakdown = iterator.next();
                final Breakdown b = new Breakdown(breakdown.getWhat(), stop - breakdown.getTiming(), breakdown.getLevel());
                LOG.info("Breakdown stop " + b.toString());
            }
        }

        @Override
        public String toString() {
            checkMissingStop();
            return doToString(false);
        }

        public String doToString(final boolean withBreakdowns) {
            return "Timing{" +
                    "messageId='" + messageId + '\'' +
                    ", destination='" + destination + '\'' +
                    ", messageSize='" + messageSize + '\'' +
                    ", whole_request='" + wholeRequest + '\'' +
                    (withBreakdowns ? (", timingBreakdowns=" + timingBreakdowns) : "") +
                    '}';
        }

        public List<Breakdown> getBreakdowns() {
            return timingBreakdowns;
        }
    }

    public static class Breakdown implements Comparable<Breakdown> {
        private final String what;
        private final Long timing;
        private final int level;
        private final Map<String, String> data;

        public Breakdown(final String what, final Long timing, final int level) {
            this(what, timing, level, new HashMap<>());
        }

        public Breakdown(final String what, final Long timing, final int level, final Map<String, String> data) {
            this.what = what;
            this.timing = timing;
            this.level = level;
            this.data = data;
        }

        public String getWhat() {
            return what;
        }

        public Long getTiming(final TimeUnit unit) {
            return unit.convert(getTiming(), TimeUnit.NANOSECONDS);
        }

        public Long getTiming() {
            return timing;
        }

        public int getLevel() {
            return level;
        }

        public Map<String, String> getData() {
            return data;
        }

        @Override
        public String toString() {
            return "Breakdown{" +
                   "what='" + getWhat() + '\'' +
                   ", timing=" + getTiming(TimeUnit.MILLISECONDS) +
                   ", level=" + getLevel() +
                   ", data=" + getData() +
                   '}';
        }

        public String prettyPrint() {
            return String.format("%10d %s %-60s", getTiming(TimeUnit.MILLISECONDS), pad(getLevel()), getWhat());
        }

        private String pad(final int level) {
            final StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("|-");
            for (int i = 0; i < level; i++) {
                stringBuilder.append("--");
            }
            return stringBuilder.toString();
        }

        @Override
        public int compareTo(final Breakdown o) {
            return Integer.compare(level, o.getLevel());
        }
    }

    public interface RecordingCallback {
        void sendComplete(final Timing timing);
    }
}
