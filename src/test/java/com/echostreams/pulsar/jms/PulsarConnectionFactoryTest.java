package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;
import com.echostreams.pulsar.jms.client.PulsarDestination;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

public class PulsarConnectionFactoryTest {
    private String serviceUrl = "pulsar://172.16.30.107:6650";
    private ConnectionFactory factory;
    private Connection con;
    private Session session;
    private Destination topic;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        factory = new PulsarConnectionFactory("pulsar://192.168.43.88:6650");
        //((PulsarConnectionFactory)factory).initializeConfig();
        //((PulsarConnectionFactory)factory).getBuilder().groupId("PulsarConnectionFactoryTest")
        //.enableAuutoCommit("true").autoCommitInterval("1000");

        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");
    }

    @Test
    public void testSend() throws JMSException {
        con = factory.createConnection();
        session = con.createSession();
        topic = session.createTopic("test");
        executeProducerTest();
    }

    /**
     * @throws JMSException
     */
    private void executeProducerTest() throws JMSException {
        MessageProducer producer = session.createProducer(topic);

        TextMessage text = session.createTextMessage();
        text.setText("this is a test.");

        producer.send(text);
        producer.close();
        session.close();
        con.close();
    }

    //@Ignore
    @Test
    public void testReceive() throws JMSException {
		executeProducerTest();
        executeConsumerTest();
		executeConsumerTest();
    }

    /**
     * @throws JMSException
     */
    private void executeConsumerTest() throws JMSException {
        MessageConsumer consumer = session.createConsumer(topic);
        Message msg = consumer.receive(5000);

        Assert.assertNotNull(msg);
        Assert.assertEquals("this is a test.", msg.getBody(String.class));

        session.unsubscribe(((PulsarDestination) topic).getName());
        consumer.close();
    }

}
