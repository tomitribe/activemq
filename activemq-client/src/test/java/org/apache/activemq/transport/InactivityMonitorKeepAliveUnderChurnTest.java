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
package org.apache.activemq.transport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntSupplier;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that connection churn does not delay keep-alives on another connection.
 * The teardown assertion also detects the former O(n) Timer.purge() cost.
 */
public class InactivityMonitorKeepAliveUnderChurnTest {

    private static final Logger LOG = LoggerFactory.getLogger(InactivityMonitorKeepAliveUnderChurnTest.class);

    /** Write-check period of the observed connection. */
    private static final long WRITE_CHECK_TIME = 50;

    /** Allows scheduler jitter while bounding the gap observed by a peer. */
    private static final long MAX_ACCEPTABLE_GAP = WRITE_CHECK_TIME * 8;

    /** Keep-alives to observe, about two seconds of nominal traffic. */
    private static final int EXPECTED_KEEP_ALIVES = 40;

    /** Upper bound on the observation window; reaching it is already a failure. */
    private static final long OBSERVATION_TIMEOUT = 30000;

    /** Large enough to expose the former cost of purging the shared task queue. */
    private static final int IDLE_MONITORS = 100000;

    /** One thread keeps opening and closing a connection during observation. */
    private static final int CHURN_THREADS = 1;

    /** Separates the observed fast teardown from the former full-queue purge. */
    private static final long MAX_TEARDOWN_TIME = 3000;

    /** Long enough that the idle and churned monitors never actually run a check. */
    private static final long IDLE_CHECK_TIME = 600000;

    private final List<AbstractInactivityMonitor> monitors = new ArrayList<AbstractInactivityMonitor>();

    @After
    public void stopMonitors() {
        for (final AbstractInactivityMonitor monitor : monitors) {
            monitor.stopMonitorThreads();
        }
        monitors.clear();
    }

    @Test(timeout = 5000)
    public void readCheckContinuesAfterRuntimeException() throws Exception {
        final CountDownLatch secondCheck = new CountDownLatch(1);
        final AtomicInteger checks = new AtomicInteger();
        final StubInactivityMonitor monitor = monitor(new KeepAliveRecordingTransport(null, () -> {
            if (checks.incrementAndGet() == 1) {
                throw new IllegalStateException("transient receive counter failure");
            }
            secondCheck.countDown();
            return 0;
        }));
        monitor.setReadCheckTime(20);
        monitor.setWriteCheckTime(0);
        monitor.setInitialDelayTime(0);

        monitor.startMonitorThreads();

        assertTrue("A runtime exception stopped the periodic read check", secondCheck.await(1000, MILLISECONDS));
    }

    @Test(timeout = 5000)
    public void errorStopsOnlyItsReadCheck() throws Exception {
        final CountDownLatch firstCheck = new CountDownLatch(1);
        final CountDownLatch secondCheck = new CountDownLatch(1);
        final AtomicInteger checks = new AtomicInteger();
        final StubInactivityMonitor monitor = monitor(new KeepAliveRecordingTransport(null, () -> {
            if (checks.incrementAndGet() == 1) {
                firstCheck.countDown();
                throw new AssertionError("fatal receive counter failure");
            }
            secondCheck.countDown();
            return 0;
        }));
        monitor.setReadCheckTime(20);
        monitor.setWriteCheckTime(0);
        monitor.setInitialDelayTime(0);

        monitor.startMonitorThreads();

        assertTrue("The read check did not run", firstCheck.await(1000, MILLISECONDS));
        final CountDownLatch otherCheck = new CountDownLatch(1);
        final StubInactivityMonitor otherMonitor = monitor(new KeepAliveRecordingTransport(null, () -> {
            otherCheck.countDown();
            return 0;
        }));
        otherMonitor.setReadCheckTime(20);
        otherMonitor.setWriteCheckTime(0);
        otherMonitor.setInitialDelayTime(0);
        otherMonitor.startMonitorThreads();

        assertTrue("The Error stopped the shared read scheduler", otherCheck.await(1000, MILLISECONDS));
        assertFalse("An Error should stop this periodic read check", secondCheck.await(500, MILLISECONDS));
    }

    @Test(timeout = 300000)
    public void keepAlivesSurviveConnectionChurn() throws Exception {
        for (int i = 0; i < IDLE_MONITORS; i++) {
            startedQuietMonitor();
        }

        final List<AbstractInactivityMonitor> churned = new ArrayList<AbstractInactivityMonitor>();
        for (int i = 0; i < CHURN_THREADS; i++) {
            churned.add(quietMonitor());
        }

        final CountDownLatch keepAlives = new CountDownLatch(EXPECTED_KEEP_ALIVES);
        final KeepAliveRecordingTransport observed = new KeepAliveRecordingTransport(keepAlives);
        final StubInactivityMonitor observer = monitor(observed);
        observer.setWriteCheckTime(WRITE_CHECK_TIME);
        observer.setInitialDelayTime(WRITE_CHECK_TIME);

        final AtomicBoolean churning = new AtomicBoolean(true);
        final AtomicReference<Throwable> churnFailure = new AtomicReference<Throwable>();
        final List<Thread> churnThreads = new ArrayList<Thread>();
        for (final AbstractInactivityMonitor monitor : churned) {
            final Thread thread = new Thread(() -> {
                try {
                    while (churning.get()) {
                        monitor.startMonitorThreads();
                        monitor.stopMonitorThreads();
                    }
                } catch (final Throwable error) {
                    churnFailure.compareAndSet(null, error);
                }
            }, "connection-churn");
            thread.setDaemon(true);
            churnThreads.add(thread);
        }

        final long observationStart;
        try {
            for (final Thread thread : churnThreads) {
                thread.start();
            }
            observationStart = System.nanoTime();
            observer.startMonitorThreads();

            // No sleep: the window closes as soon as the expected keep-alives arrive, and the
            // timeout is only there so that total starvation still ends the test.
            if (!keepAlives.await(OBSERVATION_TIMEOUT, MILLISECONDS)) {
                LOG.warn("Only {} of {} keep-alives were sent within {}ms",
                    EXPECTED_KEEP_ALIVES - keepAlives.getCount(), EXPECTED_KEEP_ALIVES, OBSERVATION_TIMEOUT);
            }
        } finally {
            churning.set(false);
            for (final Thread thread : churnThreads) {
                thread.join(30000);
            }
        }

        if (churnFailure.get() != null) {
            throw new AssertionError("Connection churn failed", churnFailure.get());
        }

        final long observationEnd = System.nanoTime();
        final List<Long> timestamps = observed.keepAliveTimestamps();
        final long largestGap = largestGap(observationStart, observationEnd, timestamps);

        final long teardownStart = System.nanoTime();
        stopMonitors();
        final long teardown = NANOSECONDS.toMillis(System.nanoTime() - teardownStart);

        LOG.info("{} keep-alives in {}ms, largest gap {}ms, teardown of {} monitors {}ms",
            timestamps.size(), NANOSECONDS.toMillis(observationEnd - observationStart),
            largestGap, IDLE_MONITORS + CHURN_THREADS + 1, teardown);

        assertTrue("The shared write-check thread was starved by connection churn: the quiet "
                + "connection went " + largestGap + "ms without a keep-alive, which is more than "
                + MAX_ACCEPTABLE_GAP + "ms (" + (MAX_ACCEPTABLE_GAP / WRITE_CHECK_TIME) + " write-check "
                + "periods of " + WRITE_CHECK_TIME + "ms). " + timestamps.size() + " of "
                + EXPECTED_KEEP_ALIVES + " keep-alives were sent in "
                + NANOSECONDS.toMillis(observationEnd - observationStart) + "ms.",
            largestGap <= MAX_ACCEPTABLE_GAP);

        assertTrue("Closing " + (IDLE_MONITORS + CHURN_THREADS + 1) + " connections took "
                + teardown + "ms, more than the " + MAX_TEARDOWN_TIME + "ms budget. Closing one "
                + "connection is purging the task queue of every other connection.",
            teardown <= MAX_TEARDOWN_TIME);
    }

    /**
     * Largest delay, in milliseconds, between two consecutive keep-alives over the window.
     * The window bounds count as events so that a monitor that sends nothing, or stops
     * sending halfway through, is measured rather than ignored.
     */
    private static long largestGap(final long start, final long end, final List<Long> timestamps) {
        long largest = 0;
        long previous = start;
        for (final Long timestamp : timestamps) {
            largest = Math.max(largest, timestamp - previous);
            previous = timestamp;
        }
        return NANOSECONDS.toMillis(Math.max(largest, end - previous));
    }

    private void startedQuietMonitor() throws IOException {
        quietMonitor().startMonitorThreads();
    }

    private AbstractInactivityMonitor quietMonitor() {
        final StubInactivityMonitor monitor = monitor(new KeepAliveRecordingTransport(null));
        monitor.setUseKeepAlive(false);
        monitor.setWriteCheckTime(IDLE_CHECK_TIME);
        monitor.setInitialDelayTime(IDLE_CHECK_TIME);
        return monitor;
    }

    private StubInactivityMonitor monitor(final Transport next) {
        final StubInactivityMonitor monitor = new StubInactivityMonitor(next);
        monitor.setTransportListener(new DefaultTransportListener());
        // Only the write check is under study; a read check would fail these stub transports.
        monitor.setReadCheckTime(0);
        monitors.add(monitor);
        return monitor;
    }

    /**
     * Monitor over a stub transport: no wire format negotiation, always configured.
     */
    private static final class StubInactivityMonitor extends AbstractInactivityMonitor {

        private StubInactivityMonitor(final Transport next) {
            super(next, null);
        }

        @Override
        protected void processInboundWireFormatInfo(final WireFormatInfo info) {
            // no wire format negotiation in this test
        }

        @Override
        protected void processOutboundWireFormatInfo(final WireFormatInfo info) {
            // no wire format negotiation in this test
        }

        @Override
        protected boolean configuredOk() {
            return true;
        }
    }

    /**
     * Stands in for a connected peer and timestamps every keep-alive it is given.
     */
    private static final class KeepAliveRecordingTransport extends TransportSupport {

        private final List<Long> timestamps = new ArrayList<Long>();
        private final CountDownLatch received;
        private final IntSupplier receiveCounter;

        private KeepAliveRecordingTransport(final CountDownLatch received) {
            this(received, () -> 0);
        }

        private KeepAliveRecordingTransport(final CountDownLatch received, final IntSupplier receiveCounter) {
            this.received = received;
            this.receiveCounter = receiveCounter;
        }

        private List<Long> keepAliveTimestamps() {
            synchronized (timestamps) {
                return new ArrayList<Long>(timestamps);
            }
        }

        @Override
        public void oneway(final Object command) {
            if (command instanceof KeepAliveInfo) {
                synchronized (timestamps) {
                    timestamps.add(System.nanoTime());
                }
                if (received != null) {
                    received.countDown();
                }
            }
        }

        @Override
        public String getRemoteAddress() {
            return "stub://peer";
        }

        @Override
        public int getReceiveCounter() {
            return receiveCounter.getAsInt();
        }

        @Override
        public X509Certificate[] getPeerCertificates() {
            return null;
        }

        @Override
        public void setPeerCertificates(final X509Certificate[] certificates) {
            // not used by the inactivity monitor
        }

        @Override
        public WireFormat getWireFormat() {
            return null;
        }

        @Override
        protected void doStart() {
            // nothing to start
        }

        @Override
        protected void doStop(final ServiceStopper stopper) {
            // nothing to stop
        }
    }
}
