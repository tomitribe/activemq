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


<h2>Hello <c:out value="${requestContext.consumerQuery.subscription.subscriptionId}" /></h2>
<h2>Hello <c:out value="${requestContext.consumerQuery.subscription.destinationName}" /></h2>
<h2>Hello <c:out value="${requestContext.consumerQuery.subscription.clientId}" /></h2>

<%@include file="decorators/footer.jsp" %>

</body>
</html>
	
