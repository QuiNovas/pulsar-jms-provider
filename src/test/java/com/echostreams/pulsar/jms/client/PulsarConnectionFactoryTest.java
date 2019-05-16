package com.echostreams.pulsar.jms.client;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import static org.junit.Assert.*;

public class PulsarConnectionFactoryTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionFactoryTest.class);

    @Test
    public void testCreateConnection() throws JMSException {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("mock://127.0.0.1:6650");

        Connection connection = factory.createConnection();
        assertNotNull(connection);

        try {
            connection.close();
            fail("Should have thrown exception");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testCreateQueueConnection() throws JMSException {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("mock://127.0.0.1:6650");

        QueueConnection connection = factory.createQueueConnection();
        assertNotNull(connection);
        try {
            connection.close();
            fail("Should have thrown exception");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testCreateTopicConnection() throws JMSException {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("mock://127.0.0.1:6650");

        TopicConnection connection = factory.createTopicConnection();
        assertNotNull(connection);

        try {
            connection.close();
            fail("Should have thrown exception");
        } catch (NullPointerException npe) {
        }
    }

    public void testCreateConnectionWithPortOutOfRange() throws Exception {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("pulsar://127.0.0.1:665066506");

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException iae) {
            LOGGER.debug("Exceiption Occurred : ", iae);
        }

        factory = new PulsarConnectionFactory("pulsar://127.0.0.1:665066506");

        try {
            factory.createConnection();
            fail("Should have thrown exception");
        } catch (IllegalArgumentException iae) {
            LOGGER.debug("Exceiption Occurred : ", iae);
        }
    }

    @Test
    public void testCreateContext() {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("mock://127.0.0.1:6650");

        JMSContext context = factory.createContext();
        assertNotNull(context);
        assertEquals(JMSContext.AUTO_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }

    @Test
    public void testCreateContextWithSessionMode() {
        PulsarConnectionFactory factory = new PulsarConnectionFactory("mock://127.0.0.1:6650");

        JMSContext context = factory.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
        assertNotNull(context);
        assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, context.getSessionMode());

        context.close();
    }

}
