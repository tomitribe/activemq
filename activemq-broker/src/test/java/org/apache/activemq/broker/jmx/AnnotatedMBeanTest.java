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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import javax.management.MBeanAttributeInfo;
import javax.management.MBeanOperationInfo;
import javax.management.ObjectName;

import org.junit.Test;

public class AnnotatedMBeanTest {

    public interface SampleViewMBean {
        @MBeanInfo("Number of things")
        long getCount();

        @MBeanInfo("Whether it runs")
        boolean isActive();

        @MBeanInfo("Purge everything")
        void purge();

        @MBeanInfo("Purge one destination")
        void purge(@MBeanInfo("destinationName") String destination);

        @MBeanInfo("Purge by ids")
        void purge(@MBeanInfo("ids") long[] ids, @MBeanInfo("selectors") String[] selectors);

        int undocumented(int value);
    }

    public static class SampleView implements SampleViewMBean {
        @Override
        public long getCount() {
            return 0;
        }

        @Override
        public boolean isActive() {
            return true;
        }

        @Override
        public void purge() {
        }

        @Override
        public void purge(final String destination) {
        }

        @Override
        public void purge(final long[] ids, final String[] selectors) {
        }

        @Override
        public int undocumented(final int value) {
            return value;
        }
    }

    @Test
    public void descriptionsComeFromAnnotations() throws Exception {
        final javax.management.MBeanInfo info = mbean().getMBeanInfo();

        assertEquals("Number of things", attribute(info, "Count").getDescription());
        assertEquals("Whether it runs", attribute(info, "Active").getDescription());
        assertEquals("Purge everything", operation(info, "purge").getDescription());
        assertEquals("Purge one destination", operation(info, "purge", "java.lang.String").getDescription());
        assertEquals("Purge by ids", operation(info, "purge", "[J", "[Ljava.lang.String;").getDescription());
    }

    @Test
    public void parameterNamesComeFromAnnotations() throws Exception {
        final javax.management.MBeanInfo info = mbean().getMBeanInfo();

        assertEquals("destinationName", operation(info, "purge", "java.lang.String").getSignature()[0].getName());
        final MBeanOperationInfo arrays = operation(info, "purge", "[J", "[Ljava.lang.String;");
        assertEquals("ids", arrays.getSignature()[0].getName());
        assertEquals("selectors", arrays.getSignature()[1].getName());
    }

    @Test
    public void undocumentedOperationKeepsDefaults() throws Exception {
        final javax.management.MBeanInfo info = mbean().getMBeanInfo();
        final MBeanOperationInfo undocumented = operation(info, "undocumented", "int");

        assertNotNull(undocumented);
        assertEquals("Operation exposed for management", undocumented.getDescription());
        assertEquals("p1", undocumented.getSignature()[0].getName());
    }

    private static AnnotatedMBean mbean() throws Exception {
        return new AnnotatedMBean(new SampleView(), SampleViewMBean.class, new ObjectName("test:type=Sample"));
    }

    private static MBeanAttributeInfo attribute(final javax.management.MBeanInfo info, final String name) {
        return Arrays.stream(info.getAttributes())
                .filter(attribute -> attribute.getName().equals(name))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No attribute " + name));
    }

    private static MBeanOperationInfo operation(final javax.management.MBeanInfo info, final String name, final String... types) {
        return Arrays.stream(info.getOperations())
                .filter(operation -> operation.getName().equals(name))
                .filter(operation -> Arrays.equals(types, Arrays.stream(operation.getSignature()).map(p -> p.getType()).toArray(String[]::new)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("No operation " + name + Arrays.toString(types)));
    }
}
