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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.command.KeepAliveInfo;
import org.apache.activemq.command.WireFormatInfo;
import org.apache.activemq.util.ServiceStopper;
import org.apache.activemq.wireformat.WireFormat;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Every {@link AbstractInactivityMonitor} of a JVM shares one write-check thread, so a
 * connection that is being torn down must not be able to stop that thread from sending
 * keep-alives on the connections that stay up.
 *
 * <p>Before the fix, starting and stopping a monitor called {@code Timer.purge()} three
 * times per connection lifecycle. {@code purge()} is O(number of live connections) and it
 * runs under the {@code Timer}'s own queue lock, so closing connections in a loop froze the
 * shared write-check thread: no keep-alive was sent on any connection for as long as the
 * churn lasted. Clients then hit their own read check and disconnected, which is the
 * mass-reconnect storm observed in production at 25.000 connections.</p>
 *
 * <p>The test measures the only thing a peer can observe: the delay between two
 * {@link KeepAliveInfo} commands handed to the transport of one quiet connection, while
 * another connection is opened and closed as fast as the machine allows.</p>
 */
public class InactivityMonitorKeepAliveUnderChurnTest {

    private static final Logger LOG = LoggerFactory.getLogger(InactivityMonitorKeepAliveUnderChurnTest.class);

    /** Write-check period of the observed connection. */
    private static final long WRITE_CHECK_TIME = 50;

    /**
     * A keep-alive may legitimately be late: the thread is shared and the machine is loaded.
     * Ten periods is far more slack than a healthy monitor ever needs, and far less than the
     * starvation the defect produces. Measured on the fixed code the gap stays near one
     * period; measured on the pre-fix code it is above one second.
     */
    private static final long MAX_ACCEPTABLE_GAP = WRITE_CHECK_TIME * 10;

    /** Keep-alives to observe, about two seconds of nominal traffic. */
    private static final int EXPECTED_KEEP_ALIVES = 40;

    /** Upper bound on the observation window; reaching it is already a failure. */
    private static final long OBSERVATION_TIMEOUT = 30000;

    /**
     * Live connections holding tasks in the shared queue, which is what makes a purge
     * expensive. Do not lower this number: the starvation is linear in it, and the test stops
     * being able to fail well before the count looks unreasonable. Largest gap measured
     * against the pre-fix code, one churn thread: 4.000 monitors gives 61ms, 20.000 gives
     * 391ms, 40.000 gives 729ms, 50.000 gives 1.121ms to 1.562ms. Only the last clears the
     * 500ms bound, so a population of 20.000 would quietly turn this into a test that passes
     * against the broken code.
     *
     * <p>The cost is modest: 40MB live after a full GC, against the 512MB this module gives
     * surefire, and about two seconds of the test's runtime.</p>
     */
    private static final int IDLE_MONITORS = 50000;

    /**
     * Connections being opened and closed in a loop. One is enough, and is in fact the worst
     * case: the teardown path is serialised on {@code AbstractInactivityMonitor.class}, so
     * extra closing threads do not purge in parallel, they only take turns on that lock and
     * each hand-off is a window for the timer thread to get back in. Measured against the
     * pre-fix code at 20.000 idle monitors: 16 churn threads starve the quiet connection for
     * 159ms, a single churn thread starves it for 391ms.
     */
    private static final int CHURN_THREADS = 1;

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

        LOG.info("{} keep-alives in {}ms, largest gap {}ms", timestamps.size(),
            NANOSECONDS.toMillis(observationEnd - observationStart), largestGap);

        assertTrue("The shared write-check thread was starved by connection churn: the quiet "
                + "connection went " + largestGap + "ms without a keep-alive, which is more than "
                + MAX_ACCEPTABLE_GAP + "ms (" + (MAX_ACCEPTABLE_GAP / WRITE_CHECK_TIME) + " write-check "
                + "periods of " + WRITE_CHECK_TIME + "ms). " + timestamps.size() + " of "
                + EXPECTED_KEEP_ALIVES + " keep-alives were sent in "
                + NANOSECONDS.toMillis(observationEnd - observationStart) + "ms.",
            largestGap <= MAX_ACCEPTABLE_GAP);
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

        private KeepAliveRecordingTransport(final CountDownLatch received) {
            this.received = received;
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
            return 0;
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
