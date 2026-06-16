Apache ActiveMQ 6.0.x-TT.x

 Changes in ActiveMQ EAP 6.0.2-TT.15
 - CVE-2026-42253 - Apache ActiveMQ's MessageServlet copies JMS message properties into HTTP response headers without input validation, enabling header injectionttacks.
 - CVE-2026-42588 - Authenticated attackers can exploit ActiveMQ's Jolokia API via a crafted masterslave:// URI to load a malicious Spring context and execute code.
 - CVE-2026-45505 - ActiveMQ's Jolokia RCE fix (CVE-2026-34197) is bypassed by non-parenthesized discovery wrappers, enabling authenticated code execution.
 - CVE-2026-46605 - Incomplete authorization in Apache ActiveMQ allows authenticated users to remove message destinations beyond their permitted access.
 - CVE-2026-49157 - Incorrect default permissions in Apache ActiveMQ grant low-privileged web users unintended access to Jolokia broker management operations.
 - CVE-2026-49270 - Apache ActiveMQ brokers expose durable subscription metadata to unauthenticated requests via BrokerInfo when syncDurableSubs is enabled.

 Changes in ActiveMQ EAP 6.0.2-TT.14
   - Upgrade to Camel 4.14.7 to address CVE-2026-40453 and CVE-2026-40860

 Changes in ActiveMQ EAP 6.0.2-TT.11
  - CVE-2025-11143: Upgrade to jetty 11.0.27-TT.1
  - CVE-2025-66168: Improper validation of remaining length which may lead to an overflow during the decoding of malformed packets
  - CVE-2026-34477: Upgrade to Log4J 2.25.4

 Changes in ActiveMQ EAP 6.0.2-TT.10
  - CVE-2025-68161: Log4J Socket Appender does not properly perform TLS hostname verification of the peer certificate
  - sonatype-2026-000642: Denial of Service (DoS) in jackson-core

 Changes in ActiveMQ EAP 6.0.2-TT.9
  - Update Shiro to 1.13.0-TT.2

 Apache ActiveMQ 6.0.2-TT.8
  - CVE-2026-23901: Observable Timing Discrepancy vulnerability in Apache Shiro
  - CVE-2026-23903: Apache Shiro [ActiveMQ] Auth bypass when accessing static files only on case-insensitive filesystems

 Apache ActiveMQ 6.0.2-TT.7
  - CVE-2025-41249: Spring Framework Annotation Detection Vulnerability

 Apache ActiveMQ 6.0.2-TT.6
  - CVE-2025-41242 - Spring Path traversal vulnerability on non-compliant Servlet containers

 Apache ActiveMQ 6.0.2-TT.5
  - CVE-2025-41234 -  Spring RFD Attack via “Content-Disposition” Header Sourced from Request

 Apache ActiveMQ 6.0.2-TT.4
  - CVE-2025-22233 - Spring Framework DataBinder Case Sensitive Match Exception (2nd update)
  - CVE-2025-48734 - Apache Commons BeanUtils PropertyUtilsBean does not suppress an enum's declaredClass property by default

 Apache ActiveMQ 6.0.2-TT.3
  - CVE-2025-27533 Out Of Memory error during openwire unmarshalling

 Apache ActiveMQ 6.0.2-TT.2
  - CVE-2025-29891 Bypass/Injection vulnerability in Apache Camel
  - CVE-2025-27636 Bypass/Injection vulnerability in Apache Camel components under particular conditions.

 Apache ActiveMQ 6.0.2-TT.1
  - CVE-2024-47072 - Mitigate XStream StackOverflowError

 Apache ActiveMQ 6.0.1-TT.2
  - CVE-2024-38820 - Spring Framework DataBinder Case Sensitive Match Exception
  - CVE-2024-38819 - Spring Path traversal vulnerability in functional web frameworks (2nd report)
  - CVE-2024-8184 - Jetty's ThreadLimitHandler.getRemote() DOS

 Apache ActiveMQ 6.0.1-TT.1
  - Sonatype-2024-3350 Apache commons-collections Denial of Service (DoS)
  - CVE-2024-38816 Spring path traversal vulnerability in functional web frameworks
