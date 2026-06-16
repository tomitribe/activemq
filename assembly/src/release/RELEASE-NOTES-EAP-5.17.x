 Apache ActiveMQ 5.17.x-TT.x

Changes in ActiveMQ EAP 5.17.8-TT.11
 - CVE-2026-42253 - Apache ActiveMQ's MessageServlet copies JMS message properties into HTTP response headers without input validation, enabling header injectionttacks.
 - CVE-2026-42588 - Authenticated attackers can exploit ActiveMQ's Jolokia API via a crafted masterslave:// URI to load a malicious Spring context and execute code.
 - CVE-2026-45505 - ActiveMQ's Jolokia RCE fix (CVE-2026-34197) is bypassed by non-parenthesized discovery wrappers, enabling authenticated code execution.
 - CVE-2026-46605 - Incomplete authorization in Apache ActiveMQ allows authenticated users to remove message destinations beyond their permitted access.
 - CVE-2026-49157 - Incorrect default permissions in Apache ActiveMQ grant low-privileged web users unintended access to Jolokia broker management operations.
 - CVE-2026-49270 - Apache ActiveMQ brokers expose durable subscription metadata to unauthenticated requests via BrokerInfo when syncDurableSubs is enabled.


Changes in ActiveMQ EAP 5.17.8-TT.10
 - Fix for CVE-2026-40466
 - Fix for CVE-2026-41043
 - Fix for CVE-2026-41044

Changes in ActiveMQ EAP 5.17.8-TT.9
 - Fix for CVE-2026-33227: Apache ActiveMQ: improper limitation of a pathname to a restricted classpath directory (path validation flaw in the classpath).
 - Fix for CVE-2026-34197: Apache ActiveMQ Classic: authenticated (and in some configurations unauthenticated) remote code execution via the Jolokia API's addNetworkConnector operation, which could be abused to load a remote Spring XML configuration and execute arbitrary OS commands.
 - Fix for CVE-2026-40046: Apache ActiveMQ: integer overflow in the MQTT control-packet "Remaining Length" decoder (a regression of CVE-2025-66168 that was missed on the 6.x branch), enabling protocol desynchronization, command smuggling, and denial of service.
 - Fix for CVE-2026-39304: Apache ActiveMQ: incorrect handling of the TLSv1.3 KeyUpdate message allows a client to trigger repeated key updates and exhaust broker memory in the SSL engine, causing denial of service; older TLS versions could also be made to hang during renegotiation.
 - Fix for CVE-2026-34478: Apache Log4j Core (Rfc5424Layout, 2.21.0â€“2.25.3): log-injection via CRLF sequences caused by silent renames of the newLineEscape and useTlsMessageFormat configuration attributes, which broke newline escaping for RFC 6587 TCP framing and silently downgraded RFC 5425 TLS framing to unframed TCP.
 - Fix for CVE-2026-34479: Apache Log4j Core: the Log4j1XmlLayout from the Log4j 1-to-2 bridge fails to escape characters forbidden by the XML 1.0 standard, producing malformed XML log output.
 - Fix for CVE-2025-66168: Apache ActiveMQ: the MQTT control-packet "Remaining Length" field is not properly validated, allowing a crafted packet to trigger malformed length handling and denial of service in the broker.

Changes in ActiveMQ EAP 5.17.8-TT.8
  - CVE-2025-11143: Upgrade to jetty 11.0.27-TT.1
  - CVE-2025-66168: Improper validation of remaining length which may lead to an overflow during the decoding of malformed packets
  - CVE-2026-34477: Upgrade to Log4J 2.25.4
  - CVE-2026-24308: Not affected (Sensitive information can be exposed in the client's logfile)
  - CVE-2026-24281: Not affected (PTR records can be manipulated to impersonate ZooKeeper servers and clients)

Changes in ActiveMQ EAP 5.17.8-TT.7
  - CVE-2025-68161: Log4J Socket Appender does not properly perform TLS hostname verification of the peer certificate
  - sonatype-2026-000642: Denial of Service (DoS) in jackson-core

Changes in ActiveMQ EAP 5.17.8-TT.6
  - CVE-2026-23901: Observable Timing Discrepancy vulnerability in Apache Shiro
  - CVE-2026-23903: Apache Shiro [ActiveMQ] Auth bypass when accessing static files only on case-insensitive filesystems

Changes in TomEE EAP 5.17.8-TT.4
  - CVE-2025-41249 - Spring Framework Annotation Detection Vulnerability
  - Additional debug logs for identifying client connection issues

Changes in TomEE EAP 5.17.8-TT.3
  - Updated to Spring 5.3.39.RELEASE-TT.5 to mitigate CVE-2025-41242

 Changes in ActiveMQ EAP 5.17.8-TT.2
  - CVE-2025-22233 - Spring Framework DataBinder Case Sensitive Match Exception (2nd update)
  - CVE-2024-13009 - Jetty Improper Resource Shutdown or Release
  - CVE-2025-48734 - Apache Commons BeanUtils PropertyUtilsBean does not suppress an enum's declaredClass property by default

 Changes in ActiveMQ EAP 5.17.8-TT.1
  - Merged ActiveMQ Classic 5.17.x maintenance release.

 Changes in ActiveMQ EAP 5.17.6-TT.8
  - CVE-2024-38820 - Spring Framework DataBinder Case Sensitive Match Exception
  - CVE-2024-38819 - Spring Path traversal vulnerability in functional web frameworks (2nd report)
  - CVE-2024-8184 - Jetty's ThreadLimitHandler.getRemote() DOS
  - CVE-2024-6762 - Jetty PushSessionCacheFilter DOS

 Changes in ActiveMQ EAP 5.17.6-TT.7
  - This release supersedes version ActiveMQ 5.17.6-TT.6, providing stable binary.

 Changes in ActiveMQ EAP 5.17.6-TT.6
  - Sonatype-2024-3350 - Apache commons-collections Denial of Service (DoS).
  - CVE-2024-38816 - Spring path traversal vulnerability in functional web frameworks.

 Changes in ActiveMQ EAP 5.17.6-TT.5
  - CVE-2024-38809 - Spring Framework DoS via conditional HTTP request
  - CVE-2024-38808 - Spring Expression DoS Vulnerability

 Changes in ActiveMQ EAP 5.17.6-TT.4
  - CVE-2024-22262 - Spring Framework URL Parsing with Host Validation (3rd report)
  - CVE-2024-22259 - Spring Framework URL Parsing with Host Validation (2nd report)

 Changes in ActiveMQ EAP 5.17.6-TT.3
  - CVE-2024-22243 - Spring Framework URL Parsing with Host Validation
  - CVE-2024-22201 - Jetty leaked HTTP/2 TCP connection.
