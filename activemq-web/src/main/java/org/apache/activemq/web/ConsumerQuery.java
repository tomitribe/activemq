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
package org.apache.activemq.web;

import org.apache.activemq.broker.jmx.SubscriptionViewMBean;

import java.util.Collection;
import java.util.Collections;

/**
 * Query for a single connection.
 * 
 * @author ms
 */
public class ConsumerQuery {

	private final BrokerFacade mBrokerFacade;
	private String mConsumerID;

	public ConsumerQuery(BrokerFacade brokerFacade) {
		mBrokerFacade = brokerFacade;
	}

	public void destroy() {
		// empty
	}

	public void setConsumerID(String consumerID) {
		mConsumerID = consumerID;
	}

	public String getConsumerID() {
		return mConsumerID;
	}

	public SubscriptionViewMBean getSubscription() throws Exception {



		String consumerID = getConsumerID();
		if (consumerID == null)
			return null;

		return mBrokerFacade.findConsumerId(consumerID);
	}

}