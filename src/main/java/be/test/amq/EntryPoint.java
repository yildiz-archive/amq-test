package be.test.amq;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * @author Grégory Van den Borre
 */
public class EntryPoint {

    public EntryPoint() {
        super();
    }

    public static void main(String[] args) throws JMSException {
        consumer();
    }

    private static void consumer() throws JMSException {
        var context = new AmqContext();
        var consumer = context.createConsumer();
        long now = System.currentTimeMillis();
        int current = 0;
        long max = 57859;
        while(current < max) {
            var msg = consumer.receive();
            current++;
        }
        context.close();
    }

    private static void producer() throws JMSException{
        var context = new AmqContext();
        var producer = context.createProducer();

        int messages = 10_000;
        long now = System.currentTimeMillis();


        for( int i=1; i <= messages; i ++) {
            var msg = context.session.createTextMessage("azerty");
            producer.send(msg);
            if( (i % 1000) == 0) {
                System.out.printf("Sent %d messages%n", i);
            }
        }
        long executionTime = System.currentTimeMillis() - now;

        System.out.printf("Total time %d ms%n", executionTime);
        System.out.printf("Average message time %f ms%n", (double) executionTime /messages);
        System.out.println("Closing");
        context.close();
    }

    private static class AmqContext {

        private final Connection connection;
        private final Session session;
        private final ActiveMQQueue dest;

        private AmqContext() throws JMSException {
            var factory = new ActiveMQConnectionFactory("tcp://localhost:61616");

            this.connection = factory.createConnection("admin", "admin");
            connection.start();
            this.session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            this.dest = new ActiveMQQueue("test");
        }

        public MessageProducer createProducer() throws JMSException {
            return session.createProducer(dest);
        }

        public MessageConsumer createConsumer() throws JMSException {
            return session.createConsumer(dest);
        }

        public void close() throws JMSException {
            connection.close();
        }
    }
}
