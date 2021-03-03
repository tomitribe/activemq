package org.apache.activemq.plugin;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.jmx.AnnotatedMBean;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.MBeanInfo;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.PrefetchSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.JMException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Simple ActiveMQ Broker Plugin to expose additional information on subscriptions through JMX.
 *
 * At present, this exposes the following attributes for subscriptions:
 *
 * additionalPredicate: This is a JmsSelector that is not part of the "regular" selector, and is typically used
 * as a filter for security purposes
 *
 * unackedMessageIds: This returns an array of MessageIds (as Strings) for all messages that the consumer has received,
 * but not acknowledged. Typically this can be reproduced by doing Message message = consumer.receive(), and *not* calling
 * message.acknowledge().
 *
 * This plugin registers and de-registers MBeans as subscriptions are added and removed from the broker. It does not
 * require any parameters.
 *
 * The classes in this file can be split into separate .java files if you wish, they are all here to enable this plugin
 * source to be provided in one single file.
 */
public class SubscriptionDispatchedMessagesPlugin implements BrokerPlugin {

    private static final Logger logger = LoggerFactory.getLogger(SubscriptionDispatchedMessagesPlugin.class);

    /**
     * Inserts an implementation of org.apache.activemq.broker.Broker into the chain. This returns an implementation of
     * org.apache.activemq.broker.Broker that delegates to the given broker, allow us to wrap method invocations.
     * @param broker The next broker in the chain
     * @return New broker object that intercepts addConsumer, removeConsumer and removeSubscription methods
     * @throws Exception
     */
    @Override
    public Broker installPlugin(final Broker broker) throws Exception {
        logger.debug("SubscriptionDispatchedMessagesPlugin::installPlugin started");
        try {
            return new SubscriptionDispatchedMessagesBroker(broker);
        } catch (final Exception e) {
            logger.error("SubscriptionDispatchedMessagesPlugin::installPlugin error " + e.getMessage(), e);
            throw e;
        } finally {
            logger.debug("SubscriptionDispatchedMessagesPlugin::installPlugin complete");
        }
    }

    /**
     * An implementation of org.apache.activemq.broker.Broker that intercepts broker methods to provide additional
     * subscription details via JMX.
     */
    public static class SubscriptionDispatchedMessagesBroker extends BrokerFilter {

        /**
         * HashMap to track the MBeans registered against the subscriptions they relate to. This allows MBeans to be
         * unregistered and cleaned up appropriately
         */
        private Map<Subscription, ObjectName> registeredMBeans = new ConcurrentHashMap<>();

        /**
         * Constructor which takes in the next broker plugin in the chain
         * @param broker The broker object to delegate to
         */
        public SubscriptionDispatchedMessagesBroker(final Broker broker) {
            super(broker);
        }

        /**
         * Called when a subscription to a destination is added. This method delegates to the broker, and register an additional
         * org.apache.activemq.plugin.SubscriptionDispatchedMessagesPlugin.SubscriptionAdditionalInfoViewMBean MBean for the
         * subscription that is created.
         * @param context connection context
         * @param info consumer information
         * @return The subscription created
         * @throws Exception
         */
        @Override
        public Subscription addConsumer(final ConnectionContext context, final ConsumerInfo info) throws Exception {
            logger.debug("SubscriptionDispatchedMessagesBroker::addConsumer started");
            try {
                final Subscription subscription = super.addConsumer(context, info);
                final ObjectName objectName = register(context, info, subscription);

                registeredMBeans.put(subscription, objectName);
                return subscription;
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::addConsumer error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::addConsumer complete");
            }
        }

        /**
         * Creates and registers as new JMX MBean for the given subscription. Delegates to getObjectName() to determine
         * a suitable JMX ObjectName.
         * @param context connection context
         * @param info consumer information
         * @param subscription The subscription to register an MBean for
         * @return The ObjectName for the registered MBean
         * @throws Exception
         */
        private ObjectName register(final ConnectionContext context, final ConsumerInfo info, final Subscription subscription) throws Exception {
            logger.debug("SubscriptionDispatchedMessagesBroker::register started");
            try {
                final ObjectName objectName = getObjectName(context, info);
                AnnotatedMBean.registerMBean(next.getBrokerService().getManagementContext(), new SubscriptionAdditionalInfoView(subscription), objectName);

                return objectName;
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::register error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::register complete");
            }
        }

        /**
         * Computes the ObjectName for the subscription. This delegates to
         * org.apache.activemq.broker.jmx.BrokerMBeanSupport#createSubscriptionName(javax.management.ObjectName, java.lang.String, org.apache.activemq.command.ConsumerInfo)
         * and appends "view=extended".
         * @param context connection context
         * @param info consumer information
         * @return The ObjectName to use for the subscription
         * @throws MalformedObjectNameException
         */
        private ObjectName getObjectName(final ConnectionContext context, final ConsumerInfo info) throws MalformedObjectNameException {
            logger.debug("SubscriptionDispatchedMessagesBroker::register started");
            try {
                final ObjectName subscriptionName = BrokerMBeanSupport.createSubscriptionName(next.getBrokerService().getBrokerObjectName(), context.getClientId(), info);
                final String domain = subscriptionName.getDomain();
                final Hashtable<String, String> propertyList = subscriptionName.getKeyPropertyList();
                propertyList.put("view", "extended");

                return new ObjectName("com.tomitribe.activemq", propertyList);
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::register error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::register complete");
            }
        }

        /**
         * Called when a subscription is removed. This method should remove any MBeans associated with the subscription that this
         * plugin has created.
         * @param context connection context
         * @param info consumer information
         * @throws Exception
         */
        @Override
        public void removeSubscription(final ConnectionContext context, final RemoveSubscriptionInfo info) throws Exception {
            logger.debug("SubscriptionDispatchedMessagesBroker::removeSubscription started");
            try {
                super.removeSubscription(context, info);
                removeMBean(info.getClientId(), info.getSubscriptionName());
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::removeSubscription error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::removeSubscription complete");
            }
        }

        /**
         * Searches for subscriptions with registered MBeans and deregisters MBeans found
         * @param clientId the client ID for the subscription
         * @param subscriptionName the subscription name for the subscription
         * @throws JMException
         */
        private void removeMBean(final String clientId, final String subscriptionName) throws JMException {
            logger.debug("SubscriptionDispatchedMessagesBroker::removeMBean started");
            try {
                final Set<Subscription> subscriptions = registeredMBeans.keySet();
                for (final Subscription subscription : subscriptions) {

                    if (subscription.getConsumerInfo() == null) continue;

                    if (isEqual(subscription.getConsumerInfo().getClientId(), clientId)
                        && isEqual(subscription.getConsumerInfo().getSubscriptionName(), subscriptionName))  {
                        final ObjectName objectName = registeredMBeans.remove(subscription);
                        next.getBrokerService().getManagementContext().unregisterMBean(objectName);
                    }
                }
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::removeMBean error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::removeMBean complete");
            }
        }

        /**
         * Checks 2 strings for equality. If both are null, this also returns true. Convenience method that also performs
         * null checks.
         * @param s1 string 1
         * @param s2 string 2
         * @return Return true if both inputs are null, or if both inputs are the same string.
         */
        private boolean isEqual(final String s1, final String s2) {
            if (s1 == null && s2 == null) {
                return true;
            }

            if (s1 == null || s2 == null) {
                return false;
            }

            return s1.equals(s2);
        }


        /**
         * Called when a consumer is removed. This method should remove any MBeans associated with the subscription that this
         * plugin has created.
         * @param context connection context
         * @param info consumer information
         * @throws Exception
         */
        @Override
        public void removeConsumer(final ConnectionContext context, final ConsumerInfo info) throws Exception {
            logger.debug("SubscriptionDispatchedMessagesBroker::removeConsumer started");
            try {

                super.removeConsumer(context, info);
            removeMBean(info.getClientId(), info.getSubscriptionName());
            } catch (final Exception e) {
                logger.error("SubscriptionDispatchedMessagesBroker::removeConsumer error " + e.getMessage(), e);
                throw e;
            } finally {
                logger.debug("SubscriptionDispatchedMessagesBroker::removeConsumer complete");
            }
        }
    }

    /**
     * MBean interface for the additional view
     */
    public interface SubscriptionAdditionalInfoViewMBean {

        @MBeanInfo("Dispatched but un-acknowledged message IDs")
        String[] getUnackedMessageIds();

        @MBeanInfo("Additional predicate associated with the subscription")
        String getAdditionalPredicate();

    }

    /**
     * This class contains the logic for the additional MBean to expose the Unacked Message IDs and
     * the additional predicate for the subscription (if any).
     */
    public static class SubscriptionAdditionalInfoView implements SubscriptionAdditionalInfoViewMBean {

        /**
         * The subscription related to this MBean
         */
        protected final Subscription subscription;

        public SubscriptionAdditionalInfoView(final Subscription subscription) {
            this.subscription = subscription;
        }

        /**
         * Returns the messages IDs for any messages dispatched to consumers that have not been acknowledged
         * @return The IDs un-acknowledged messages, as a string array.
         */
        @Override
        public String[] getUnackedMessageIds() {
            if (subscription instanceof PrefetchSubscription) {
                final PrefetchSubscription prefetchSubscription = (PrefetchSubscription) this.subscription;
                // what we're after appears to only be accessible via reflection

                try {
                    final Field dispatchedField = PrefetchSubscription.class.getDeclaredField("dispatched");
                    dispatchedField.setAccessible(true);
                    final List<MessageReference> dispatched = new ArrayList<>();
                    dispatched.addAll((Collection<? extends MessageReference>) dispatchedField.get(prefetchSubscription));

                    final List<String> messageIdList =
                            dispatched
                                .stream()
                                .map(mr -> mr.getMessageId().toString())
                                .collect(Collectors.toList());

                    return messageIdList.toArray(new String[0]);
                } catch (Exception e) {
                    return new String[] { "Unable to read." };
                }
            }

            return new String[] { "Not a pre-fetch subscription." };
        }

        /**
         * Returns the additional predicate for the subscription, if there is one. Otherwise, and empty string ("")
         * is returned.
         * @return The additional predicate for the subscription.
         */
        @Override
        public String getAdditionalPredicate() {
            if (subscription.getConsumerInfo() == null) {
                return "";
            }

            if (subscription.getConsumerInfo().getAdditionalPredicate() == null) {
                return "";
            }

            return subscription.getConsumerInfo().getAdditionalPredicate().toString();
        }
    }
}
