package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;
import com.echostreams.pulsar.jms.client.PulsarDestination;
import org.apache.pulsar.client.impl.conf.ProducerConfigurationData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class PulsarJMSClientProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSClientProvider.class);

    private String serviceUrl = "pulsar://172.16.30.107:6650";
    private ConnectionFactory factory = new PulsarConnectionFactory(serviceUrl);
    private Connection con;
    private Session session;
    private Destination topic;

    // Queue Test
    private QueueConnectionFactory qfactory = new PulsarConnectionFactory(serviceUrl);
    private QueueConnection qcon;
    private QueueSession qsession;
    private Queue queue;

    // Topic Test
    private TopicConnectionFactory tfactory = new PulsarConnectionFactory(serviceUrl);
    private TopicConnection tcon;
    private TopicSession tsession;
    private Topic tp;

    // Jms 2.0
    private ConnectionFactory cfactory = new PulsarConnectionFactory(serviceUrl);
    private JMSContext jmsContext;
    private Destination ctopic;

    private final ProducerConfigurationData conf = new ProducerConfigurationData();

    public static void main(String[] args) throws JMSException {
        PulsarJMSClientProvider pulsarJMSClientProvider = new PulsarJMSClientProvider();
        pulsarJMSClientProvider.executeProducerTest();
        pulsarJMSClientProvider.executeProducerByteTest();
        pulsarJMSClientProvider.executeConsumerTest();

        // Queue
        pulsarJMSClientProvider.executeQSenderTest();
        pulsarJMSClientProvider.executeQReceiverTest();

        //Topic
        pulsarJMSClientProvider.executeTopicPublisherTest();
        pulsarJMSClientProvider.executeTopicSubscriberTest();

        //JMS2-JMSContext
        pulsarJMSClientProvider.executeJms2ProducerTest();
        pulsarJMSClientProvider.executeJms2ConsumerTest();

    }

    private void executeJms2ProducerTest() {
        try {
            jmsContext = cfactory.createContext();
            ctopic = jmsContext.createTopic("test");

            jmsContext.createProducer().send(ctopic, "Hello JMS 2");
        } catch (JMSRuntimeException e) {
            LOGGER.error("JMSRuntimeException", e);
        }

    }

    private void executeJms2ConsumerTest() throws JMSException {
        try {
            jmsContext = cfactory.createContext();
            ctopic = jmsContext.createTopic("test");
            JMSConsumer consumer = jmsContext.createConsumer(ctopic);
            consumer.receive();
            jmsContext.unsubscribe(((PulsarDestination) ctopic).getName());
            consumer.close();
        } catch (JMSRuntimeException e) {
            LOGGER.error("JMSRuntimeException", e);
        }

    }

    private void executeProducerTest() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createQueue("test");

        MessageProducer producer = session.createProducer(topic);

        TextMessage text = session.createTextMessage();
        text.setText("this is a test.");

        producer.send(text);
        producer.close();
    }

    private void executeProducerByteTest() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");

        MessageProducer producer = session.createProducer(topic);

        BytesMessage text = session.createBytesMessage();
        text.writeBytes("this is a Byte test.".getBytes());

        producer.send(text);
        producer.close();
    }

    private void executeConsumerTest() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");


        MessageConsumer consumer = session.createConsumer(topic);
        consumer.receive(5000);
        session.unsubscribe(((PulsarDestination) topic).getName());
        consumer.close();
    }

    private void executeQReceiverTest() throws JMSException {
        qcon = qfactory.createQueueConnection();
        qsession = qcon.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = qsession.createQueue("test");

        QueueReceiver qr = qsession.createReceiver(queue);
        TextMessage tm = (TextMessage) qr.receiveNoWait();
        if (tm != null) {
            LOGGER.info("got text message: " + tm.getText());
            LOGGER.info("string property: " + tm.getStringProperty("string"));
            LOGGER.info("int property: " + tm.getIntProperty("int"));
            LOGGER.info("float property: " + tm.getFloatProperty("float"));
            LOGGER.info("long property: " + tm.getLongProperty("long"));
        } else {
            LOGGER.info("got nothing");
        }
        qr.close();
        qsession.close();

    }

    private void executeQSenderTest() throws JMSException {
        qcon = qfactory.createQueueConnection();
        qsession = qcon.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
        queue = qsession.createQueue("test");

        QueueSender qsend = qsession.createSender(queue);
        TextMessage tm = qsession.createTextMessage();
        tm.setText("How's it bud?");
        tm.setStringProperty("string", "yet another string");
        tm.setIntProperty("int", 456);
        tm.setFloatProperty("float", 9.82f);
        tm.setLongProperty("long", 987654321);
        qsend.send(tm);
        qsend.close();
        qsession.close();

    }

    private void executeTopicSubscriberTest() throws JMSException {
        tcon = tfactory.createTopicConnection();
        tsession = tcon.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        tp = tsession.createTopic("test");

        TopicSubscriber tsub = tsession.createSubscriber(tp);
        Message msg = tsub.receiveNoWait();
        LOGGER.info("initial message", msg);

        tsub.close();
        tsession.close();

    }

    private void executeTopicPublisherTest() throws JMSException {
        tcon = tfactory.createTopicConnection();
        tsession = tcon.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        tp = tsession.createTopic("test");

        TopicPublisher pub = tsession.createPublisher(tp);
        TextMessage om = tsession.createTextMessage("Hello Topic");
        //ObjectMessage om = tsession.createObjectMessage(new Integer(99));
        pub.publish(om);
        pub.close();
        tsession.close();

    }
}
