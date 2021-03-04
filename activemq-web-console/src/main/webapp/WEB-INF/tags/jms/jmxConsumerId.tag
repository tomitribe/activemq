<%--
    Licensed to the Apache Software Foundation (ASF) under one or more
    contributor license agreements.  See the NOTICE file distributed with
    this work for additional information regarding copyright ownership.
    The ASF licenses this file to You under the Apache License, Version 2.0
    (the "License"); you may not use this file except in compliance with
    the License.  You may obtain a copy of the License at
   
    http://www.apache.org/licenses/LICENSE-2.0
   
    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
--%>
<%@ attribute name="subscription" type="org.apache.activemq.broker.jmx.SubscriptionViewMBean " required="true"  %>
<%@ tag import="org.apache.activemq.util.JMXSupport " %>
<%@ tag import="java.net.URLEncoder" %>
<%
	if (subscription.isDurable()) {
		out.println(URLEncoder.encode("Durable(" + JMXSupport.encodeObjectNamePart(subscription.getConnectionId() + ":" + subscription.getSubscriptionName()) +")", "UTF-8"));
	} else {
		out.println(URLEncoder.encode(JMXSupport.encodeObjectNamePart(subscription.getConnectionId() + ":" + subscription.getSessionId() + ":" + subscription.getSubscriptionId()), "UTF-8"));
	}
%>

