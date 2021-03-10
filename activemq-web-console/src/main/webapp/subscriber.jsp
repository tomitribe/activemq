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
<%-- Workaround for https://ops4j1.jira.com/browse/PAXWEB-1070 --%>
<%@include file="WEB-INF/jspf/headertags.jspf" %>
<html>
<head>
<c:set var="pageTitle" value="Durable Topic Subscribers"/>

<%@include file="decorators/head.jsp" %>
</head>
<body>

<%@include file="decorators/header.jsp" %>

<h2>Subscription details</h2>

<table class="layout">
    <tr>
        <td class="layout"  valign="top">
            <table id="header" class="autostripe" width="100%">
                <thead>
                <tr>
                    <th colspan="2">
                        Subscription Details
                    </th>
                </tr>
                </thead>
                <tbody>
                <tr>
                    <td class="label" title="JMS Client id of the Connection the Subscription is on">Client ID</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.clientId}"/></td>
                </tr>
                <tr>
                    <td class="label" title="ID of the Connection the Subscription is on">Connection ID</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.connectionId}"/></td>
                </tr>
                <tr>
                    <td class="label" title="ID of the Subscription">Subscription ID</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.subscriptionId}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The name of the destination the subscription is on">Destination Name</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.destinationName}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The SQL-92 message header selector or XPATH body selector of the subscription">Selector</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.selector}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The additional predicate of the subscription">Additional Predicate</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.additionalPredicate}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Subscription is on a Queue">Queue subscription</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.destinationQueue}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Subscription is on a Topic">Topic subscription</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.destinationTopic}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Subscription is on a temporary Queue/Topic">Temporary Destination</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.destinationTemporary}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Subscription is active (connected and receiving messages)">Active</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.active}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Subscription was created by a demand-forwarding network bridge">Network</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.network}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The subscriber is retroactive (tries to receive broadcasted topic messages sent prior to connecting)">Retroactive</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.retroactive}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The subscriber is exclusive (no other subscribers may receive messages from the destination as long as this one is)">Exclusive</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.exclusive}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The subscription is persistent">Durable</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.durable}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The subscription ignores local messages">Ignore Local</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.noLocal}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Is the consumer configured for Async Dispatch">Async Dispatch</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.dispatchAsync}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The subscription priority">Priority</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.priority}"/></td>
                </tr>
                <tr>
                    <td class="label" title="The name of the subscription (durable subscriptions only)">Subscription Name</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.subscriptionName}"/></td>
                </tr>
                <tr>
                    <td class="label" title="Is the subscription slow">Slow</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.slowConsumer}"/></td>
                </tr>
                <tr>
                    <td class="label" title="User Name used to authorize creation of this Subscription">Username</td>
                    <td><c:out value="${requestContext.consumerQuery.subscription.userName}"/></td>
                </tr>
                </tbody>
            </table>
        </td>
        <td class="layout"  valign="top">
        <table id="header-stats" class="autostripe" width="100%">
            <thead>
            <tr>
                <th colspan="2">
                    Subscription Statistics
                </th>
            </tr>
            </thead>
            <tbody>

            <tr>
                <td class="label" title="Number of messages pending delivery">Pending Queue Size</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.pendingQueueSize}"/></td>
            </tr>
            <tr>
                <td class="label" title="The maximum number of pending messages allowed (in addition to the prefetch size)">Max Pending</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.maximumPendingMessageLimit}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages dispatched awaiting acknowledgement">Dispatched Queue Size</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.dispatchedQueueSize}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages dispatched awaiting acknowledgement">Dispatched Not ACKd</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.messageCountAwaitingAcknowledge}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages that sent to the client">Dispatched</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.dispatchedCounter}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages that matched the subscription">Enqueued</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.enqueueCounter}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages were sent to and acknowledge by the client">Dequeued</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.dequeueCounter}"/></td>
            </tr>
            <tr>
                <td class="label" title="Number of messages to pre-fetch and dispatch to the client">Pre-fetch size</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.prefetchSize}"/></td>
            </tr>
            <tr>
                <td class="label" title="The maximum number of pending messages allowed (in addition to the prefetch size)">Max Pending Limit</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.maximumPendingMessageLimit}"/></td>
            </tr>
            <tr>
                <td class="label" title="Messages consumed">Consumed</td>
                <td><c:out value="${requestContext.consumerQuery.subscription.consumedCount}"/></td>
            </tr>
            </tbody>
        </table>
        </td>
    </tr>
</table>
<h2>Dispatched Messages</h2>

<table class="layout">
    <tr>
        <td class="layout"  valign="top">
            <table id="msg-header" class="autostripe" width="100%">
                <thead>
                <tr>
                        <th>Message ID</th>
                        <th>Persistent</th>
                        <th>Expired</th>
                        <th>Dropped</th>
                        <th>Advisory</th>
                        <th>Delivered</th>
                        <th>Acknowledged</th>
                </tr>
                </thead>
                <tbody>
<%-- TODO: redelivery count etc --%>
                <form:forEachTablularData var="item" items="${requestContext.consumerQuery.subscription.dispatchedMessageReferences}">
                <tr>
                    <td><a href="<c:url value="message.jsp">
                            <c:param name="id" value="${item['messageId']}" />
                            <c:param name="JMSDestination" value="${requestContext.consumerQuery.subscription.destinationName}"/></c:url>"
                           title="<c:out value="${item['messageId']}"/>">${item['messageId']}
                    </a></td>
                    <td>${item['persistent']}</td>
                    <td>${item['expired']}</td>
                    <td>${item['dropped']}</td>
                    <td>${item['advisory']}</td>
                    <td>${item['delivered']}</td>
                    <td>${item['acknowledged']}</td>
                </tr>
                </form:forEachTablularData>
                </tbody>
            </table>
        </td>
    </tr>
</table>




<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
