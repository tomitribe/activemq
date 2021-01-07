package org.apache.activemq.broker.util;

import org.apache.activemq.broker.jmx.OpenTypeSupport;
import org.apache.activemq.management.TimeStatisticImpl;

import javax.management.openmbean.CompositeData;
import java.util.ArrayList;

public class AccessLogView implements AccessLogViewMBean {

    private final AccessLogPlugin plugin;

    public AccessLogView(final AccessLogPlugin plugin) {
        this.plugin = plugin;
    }

    @Override
    public boolean isEnabled() {
        return plugin.isEnabled();
    }

    @Override
    public void setEnabled(final boolean enabled) {
        plugin.setEnabled(enabled);
    }

    @Override
    public int getThreshold() {
        return plugin.getThreshold();
    }

    @Override
    public void setThreshold(final int threshold) {
        plugin.setThreshold(threshold);
    }

    @Override
    public CompositeData[] getStatistics() {
        final TimeStatisticImpl[] timeStatistics = plugin.getTimeStatistics();
        final ArrayList<CompositeData> c = new ArrayList<CompositeData>();

        for (int i = 0; i < timeStatistics.length; i++) {
            try {
                c.add(OpenTypeSupport.convert(timeStatistics[i]));

            } catch (final Throwable e) {
                // todo log something
            }
        }

        final CompositeData rc[] = new CompositeData[c.size()];
        c.toArray(rc);
        return rc;
    }

    @Override
    public void resetStatistics() {
        plugin.resetTimeStatistics();
    }
}
