// SendGadget.groovy — shows ActiveMQ's trustedPackages blocks a ysoserial-style
// commons-collections gadget by CLASS, even when org.apache.camel + java.util are
// trusted (i.e. even in the widened-trust config where the 43866 holder DOES work).
//
// SAFETY: this payload is deliberately INERT. It uses the commons-collections
// classes ysoserial's CC1 chain is built from (LazyMap + ConstantTransformer),
// but wired so nothing executes even if it were deserialized. The point is purely
// that ActiveMQ's ClassLoadingAwareObjectInputStream rejects the
// org.apache.commons.collections.* classes at resolveClass -> a real weaponised
// payload hits the exact same wall at its first commons-collections class.
//
// Run this with the SAME broker/trust config as Case 2 (camel.xml trustedPackages
// = org.apache.camel, java.util), so we're testing "does widening trust for the
// holder also let a gadget through?" — answer: no.
//
//   groovy SendGadget.groovy        (add -Duser=admin -Dpass=admin if needed)

@Grab('org.apache.activemq:activemq-client:5.15.16')
@Grab('commons-collections:commons-collections:3.2.1')
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.commons.collections.functors.ConstantTransformer
import org.apache.commons.collections.map.LazyMap
import javax.jms.Session

// An inert commons-collections object graph (the CC1 building blocks), nested
// inside a trusted java.util HashMap to also demonstrate whole-graph filtering:
// the outer HashMap is trusted (java.util), but the LazyMap value is not.
def lazy = LazyMap.decorate(new HashMap(), new ConstantTransformer("inert"))
def outer = new HashMap()
outer.put("payload", lazy)

def cf   = new ActiveMQConnectionFactory(System.getProperty('broker', 'tcp://localhost:61616'))
def user = System.getProperty('user')
def pass = System.getProperty('pass')
def q    = System.getProperty('queue', 'cve.in')

def conn = (user != null) ? cf.createConnection(user, pass) : cf.createConnection()
conn.start()
def sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
sess.createProducer(sess.createQueue(q)).send(sess.createObjectMessage(outer))
conn.close()

println "sent ObjectMessage( HashMap{ payload: LazyMap(commons-collections) } ) -> queue://${q}"
println ""
println "Expected broker log:"
println "  Forbidden class org.apache.commons.collections... — This class is not trusted"
println "  => even with org.apache.camel + java.util trusted, the gadget package is"
println "     rejected at resolveClass. The whole graph is filtered; a trusted outer"
println "     HashMap does NOT let the nested commons-collections object ride along."
