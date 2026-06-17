 Apache ActiveMQ 6.3.x-TT.x

 Changes in ActiveMQ EAP 6.3.0-TT.4
 - CVE-2026-2332 - Upgrade Jetty to 11.0.27-TT.2
 - CVE-2026-41851, CVE-2026-41852 - Upgrade Spring to 6.2.19

 Changes in ActiveMQ EAP 6.3.0-TT.3
 - Merge latest changes from Apache ActiveMQ upstream (main)
 - CVE-2026-49157 - Restrict full web console URI to admins role
 - CVE-2026-41043 - Add missing JSP escapes in web console
 - CVE-2026-49270 - Ensure connection info is processed before durable sync
 - Harden default broker, web console and Jolokia configuration (defence-in-depth for CVE-2026-49157 and the CVE-2026-34197 Jolokia RCE family)
 - Add validation for LDAP network connector URIs
 - Add validation for WireFormatInfo (OpenWire DoS hardening)
 - Ensure at most one BrokerInfo command is received per connection
 - Add https to BrokerView restricted list
 - Send advisory messages using the broker connection context
 - Improve Stomp protocol error messages and transport validation
 - Update docs and default configs for advisory topics
 - Limit platform details string to a reasonable length
 - Fix flaky ActiveDurableSubscriptionBrowseExpireTest
 - CVE-2026-43828 - Upgrade Shiro to 2.2.0
 - CVE-2026-44249, CVE-2026-47691 - Upgrade Netty to 4.2.15.Final
 - Dependency updates: Spring 6.2.18, Jackson 2.22.0, Camel 4.20.0, Karaf 4.4.11, ASM 9.10.1, JAXB 4.0.9

 Changes in ActiveMQ EAP 6.3.1-TT.2
 - CVE-2025-11143 - Upgrade to jetty 11.0.27-TT.1

 Changes in ActiveMQ EAP 6.3.1-TT.1
   - First EAP release
