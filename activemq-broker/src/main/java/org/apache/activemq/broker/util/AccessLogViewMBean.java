package org.apache.activemq.broker.util;

import org.apache.activemq.broker.jmx.MBeanInfo;

public interface AccessLogViewMBean {
    @MBeanInfo("Enabled")
    boolean isEnabled();

    void setEnabled(@MBeanInfo("enabled") final boolean enabled);


    @MBeanInfo("Threshold timing to log")
    int getThreshold();

    void setThreshold(@MBeanInfo("threshold") final int threshold);

}
