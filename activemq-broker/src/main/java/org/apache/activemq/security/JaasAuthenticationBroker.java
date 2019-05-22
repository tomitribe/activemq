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
package org.apache.activemq.security;

import java.lang.reflect.Field;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.JassCredentialCallbackHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Logs a user in using JAAS.
 *
 *
 */
public class JaasAuthenticationBroker extends AbstractAuthenticationBroker {

    private final String jassConfiguration;
    private static Logger log = LoggerFactory.getLogger(JaasAuthenticationBroker.class);

    public JaasAuthenticationBroker(Broker next, String jassConfiguration) {
        super(next);
        this.jassConfiguration = jassConfiguration;
    }

    static class JaasSecurityContext extends SecurityContext {

        private final Subject subject;

        public JaasSecurityContext(String userName, Subject subject) {
            super(userName);
            this.subject = subject;
        }

        @Override
        public Set<Principal> getPrincipals() {
            return subject.getPrincipals();
        }
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        if (context.getSecurityContext() == null) {
            // Set the TCCL since it seems JAAS needs it to find the login module classes.
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(JaasAuthenticationBroker.class.getClassLoader());

            try {
                SecurityContext s = authenticate(info.getUserName(), info.getPassword(), null);
                context.setSecurityContext(s);
                securityContexts.add(s);
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
        super.addConnection(context, info);
    }

    @Override
    public SecurityContext authenticate(String username, String password, X509Certificate[] certificates) throws SecurityException {
        SecurityContext result = null;
        JassCredentialCallbackHandler callback = new JassCredentialCallbackHandler(username, password);
        try {
            log.info("Obtaining login context for " + jassConfiguration);
            LoginContext lc = new LoginContext(jassConfiguration, callback);

            final Field field = LoginContext.class.getDeclaredField("moduleStack");
            field.setAccessible(true);
            Object[] stack = (Object[]) field.get(lc);

            for (int i = 0, stackLength = stack.length; i < stackLength; i++) {
                Object s = stack[i];
                final Field moduleField = s.getClass().getDeclaredField("entry");
                moduleField.setAccessible(true);
                final AppConfigurationEntry entry = (AppConfigurationEntry) moduleField.get(s);
                log.info("Module[" + i + "] = " + entry.getLoginModuleName());
            }

            log.info("Login context is: " + lc);

            log.info("Calling login on context");
            lc.login();
            log.info("Login successful");

            Subject subject = lc.getSubject();
            result = new JaasSecurityContext(username, subject);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new SecurityException("User name [" + username + "] or password is invalid.", ex);
        }

        return result;
    }
}
