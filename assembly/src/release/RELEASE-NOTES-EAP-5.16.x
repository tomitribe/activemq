 Apache ActiveMQ 5.16.x-TT.x

Changes in ActiveMQ EAP 5.16.9-TT.9
 - CVE-2026-42253 - Apache ActiveMQ's MessageServlet copies JMS message properties into HTTP response headers without input validation, enabling header injectionttacks.
 - CVE-2026-42588 - Authenticated attackers can exploit ActiveMQ's Jolokia API via a crafted masterslave:// URI to load a malicious Spring context and execute code.
 - CVE-2026-45505 - ActiveMQ's Jolokia RCE fix (CVE-2026-34197) is bypassed by non-parenthesized discovery wrappers, enabling authenticated code execution.
 - CVE-2026-46605 - Incomplete authorization in Apache ActiveMQ allows authenticated users to remove message destinations beyond their permitted access.
 - CVE-2026-49157 - Incorrect default permissions in Apache ActiveMQ grant low-privileged web users unintended access to Jolokia broker management operations.
 - CVE-2026-49270 - Apache ActiveMQ brokers expose durable subscription metadata to unauthenticated requests via BrokerInfo when syncDurableSubs is enabled.

 Changes in ActiveMQ EAP 5.16.9-TT.8
  - Fix for CVE-2026-40466
  - Fix for CVE-2026-41043
  - Fix for CVE-2026-41044

 Changes in ActiveMQ EAP 5.16.9-TT.7
  - CVE-2026-33227: Apache ActiveMQ Improper validation and restriction of a classpath path name vulnerability
  - CVE-2026-34197: Apache ActiveMQ Improper Input Validation, Improper Control of Generation of Code ('Code Injection') vulnerability 
  - CVE-2026-34478: Log4j2 log injection via CRLF
  - CVE-2026-34479: Log4j2 Apache Log4j 1-to-Log4j 2 bridge fails to escape characters forbidden by the XML 1.0 standard
  - CVE-2026-39304: Improper SSL/TLS session handling in NIO SSL transport
  - CVE-2026-40046: Apache ActiveMQ MQTT control packet remaining length field is not properly validated
  - AMQ-9473: Fix IntrospectionSupport to deal with SSLSocket properties

 Changes in ActiveMQ EAP 5.16.9-TT.6
  - CVE-2025-11143: Upgrade to jetty 11.0.27-TT.1
  - CVE-2025-66168: Improper validation of remaining length which may lead to an overflow during the decoding of malformed packets
  - CVE-2026-24308: Not affected (Sensitive information can be exposed in the client's logfile)
  - CVE-2026-24281: Not affected (PTR records can be manipulated to impersonate ZooKeeper servers and clients)

 Changes in ActiveMQ EAP 5.16.9-TT.5
  - sonatype-2026-000642: Denial of Service (DoS) in jackson-core

 Changes in ActiveMQ EAP 5.16.9-TT.4
  - CVE-2026-23901: Observable Timing Discrepancy vulnerability in Apache Shiro
  - CVE-2026-23903: Apache Shiro [ActiveMQ] Auth bypass when accessing static files only on case-insensitive filesystems

 Changes in ActiveMQ EAP 5.16.9-TT.2
  - CVE-2025-22233 - Spring Framework DataBinder Case Sensitive Match Exception (2nd update)
  - CVE-2024-13009 - Jetty Improper Resource Shutdown or Release
  - CVE-2025-48734 - Apache Commons BeanUtils PropertyUtilsBean does not suppress an enum's declaredClass property by default

 Changes in ActiveMQ EAP 5.16.9-TT.1
  - Merged ActiveMQ Classic 5.16.x maintenance release.
