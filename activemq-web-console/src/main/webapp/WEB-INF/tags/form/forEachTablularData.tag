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
<%@ attribute name="var" type="java.lang.String" required="true"  %>
<%@ attribute name="items" type="javax.management.openmbean.TabularData" required="true"  %>
<%@ tag import="javax.management.openmbean.CompositeData" %>
<%@ tag import="java.util.*" %>
<%



  final Iterator iter = items.keySet().iterator();
  while (iter.hasNext()) {
      final Object[] key = ((Collection) iter.next()).toArray();
      final CompositeData compositeData = items.get(key);
      final Map<String, Object> item = new HashMap<>();

      final Set<String> fields = compositeData.getCompositeType().keySet();
      for (final String field : fields) {
          item.put(field, compositeData.get(field));
      }

      request.setAttribute(var, item);
%>
<jsp:doBody/>
<%
	}
%>       
    
