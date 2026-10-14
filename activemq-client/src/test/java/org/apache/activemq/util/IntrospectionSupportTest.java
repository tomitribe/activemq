/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class IntrospectionSupportTest {

    private class DummyClass {

        private boolean trace;

        DummyClass(boolean trace) {
            this.trace = trace;
        }

        public boolean isTrace() {
            return trace;
        }

        public void setTrace(boolean trace) {
            this.trace = trace;
        }
    }

    @Test
    public void testSetPropertyPrimitiveWithWrapperValue() {
        // Wrapper value
        Boolean value = Boolean.TRUE;

        DummyClass dummyClass = new DummyClass(false);
        dummyClass.setTrace(false);

        // dummy field expects a primitive
        IntrospectionSupport.setProperty(dummyClass, "trace", value);

        assertTrue(dummyClass.isTrace());
    }

    public static class Options {
        int size;
        String name;

        public void setSize(int size) {
            this.size = size;
        }

        public void setName(String name) {
            this.name = name;
        }

        public void setPair(String first, String second) {
        }

        public void setNothing() {
        }
    }

    public static class MoreOptions extends Options {
        boolean extra;

        public void setExtra(boolean extra) {
            this.extra = extra;
        }
    }

    @Test
    public void testFindSetterMatchesSingleArgumentSettersOnly() {
        assertEquals("setSize", IntrospectionSupport.findSetterMethod(Options.class, "size").getName());
        assertNull(IntrospectionSupport.findSetterMethod(Options.class, "pair"));
        assertNull(IntrospectionSupport.findSetterMethod(Options.class, "nothing"));
        assertNull(IntrospectionSupport.findSetterMethod(Options.class, "unknown"));
    }

    @Test
    public void testFindSetterIsResolvedPerClass() {
        assertNull(IntrospectionSupport.findSetterMethod(Options.class, "extra"));
        assertEquals("setExtra", IntrospectionSupport.findSetterMethod(MoreOptions.class, "extra").getName());
        assertEquals("setSize", IntrospectionSupport.findSetterMethod(MoreOptions.class, "size").getName());
    }

    @Test
    public void testSetPropertiesRepeatedlyOnNewInstances() {
        for (int i = 0; i < 3; i++) {
            final MoreOptions options = new MoreOptions();
            final Map<String, Object> props = new HashMap<>();
            props.put("size", "4" + i);
            props.put("name", "n" + i);
            props.put("extra", "true");
            props.put("unknown", "x");

            assertTrue(IntrospectionSupport.setProperties(options, props));
            assertEquals(40 + i, options.size);
            assertEquals("n" + i, options.name);
            assertTrue(options.extra);
            assertEquals(Collections.singleton("unknown"), props.keySet());
        }
    }
}
