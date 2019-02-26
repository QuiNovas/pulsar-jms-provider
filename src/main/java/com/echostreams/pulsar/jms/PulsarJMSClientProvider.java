package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;
import com.echostreams.pulsar.jms.client.PulsarDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

public class PulsarJMSClientProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSClientProvider.class);

    private ConnectionFactory factory = new PulsarConnectionFactory();
    private Connection con;
    private Session session;
    private Destination topic;

    public static void main(String[] args) throws JMSException {
        PulsarJMSClientProvider pulsarJMSClientProvider = new PulsarJMSClientProvider();
        pulsarJMSClientProvider.executeProducerTest();
        pulsarJMSClientProvider.executeConsumerTest();
    }

    private void executeProducerTest() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");

        MessageProducer producer = session.createProducer(topic);

        TextMessage text = session.createTextMessage();
        text.setText("this is a test.");

        producer.send(text);
        producer.close();
    }

    private void executeConsumerTest() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");


        MessageConsumer consumer = session.createConsumer(topic);
        Message msg = consumer.receive(5000);
        LOGGER.info("Received message='{}' with msg-id={}", msg.getBody(String.class), msg.getJMSMessageID());
        session.unsubscribe(((PulsarDestination) topic).getName());
        consumer.close();
    }
}
