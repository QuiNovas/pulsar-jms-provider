package com.echostreams.pulsar.jms.client;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class PulsarMessageProducerTest extends PulsarConnectionTestSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageProducerTest.class);

    private PulsarSession session;
    private final MessageCompletionListener completionListener = new MessageCompletionListener();

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockPulsarClient();
        session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    @Test(timeout = 10000)
    public void testCreateProducerWithNullDestination() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertNull(producer.getDestination());
    }

    @Test(timeout = 10000)
    public void testPriorityConfig() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        producer.setPriority(6);
        assertEquals(6, producer.getPriority());
    }

    @Test(timeout = 10000)
    public void testPriorityConfigWithInvalidPriorityValues() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
        try {
            producer.setPriority(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
        try {
            producer.setPriority(10);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
        assertEquals(Message.DEFAULT_PRIORITY, producer.getPriority());
    }

    @Test(timeout = 10000)
    public void testTimeToLiveConfig() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_TIME_TO_LIVE, producer.getTimeToLive());
        producer.setTimeToLive(1000);
        assertEquals(1000, producer.getTimeToLive());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfig() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfigWithInvalidMode() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
        try {
            producer.setDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
        try {
            producer.setDeliveryMode(5);
            fail("Should have thrown an exception");
        } catch (JMSException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testDeliveryDelayConfig() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();
        assertEquals(Message.DEFAULT_DELIVERY_DELAY, producer.getDeliveryDelay());
        producer.setDeliveryDelay(2000);
        assertEquals(2000, producer.getDeliveryDelay());
    }

    @Test(timeout = 10000)
    public void testSendWithProducerNull() throws Exception {
        PulsarDestination dest = new PulsarTopic("test_topic");
        MessageProducer producer = new PulsarMessageProducer();

        TextMessage message = session.createTextMessage();
        message.setText("this is a textmsg test.");

        try {
            producer.send(dest, message);
            fail("Expected exception not thrown");
        } catch (NullPointerException npe) {
            // expected
        }
    }


    @Test(timeout = 10000)
    public void testSendWithInvalidDestinationException() throws Exception {
        MessageProducer producer = new PulsarMessageProducer();

        TextMessage message = session.createTextMessage();
        message.setText("this is a textmsg test.");

        try {
            producer.send(message, completionListener);
            fail("Expected exception not thrown");
        } catch (InvalidDestinationException npe) {
            // expected
        }

    }

    private class MessageCompletionListener implements CompletionListener {

        private final List<Message> completed = new ArrayList<>();
        private final List<Message> failed = new ArrayList<>();
        private final List<Message> totalResult = new ArrayList<>();

        @Override
        public void onCompletion(Message message) {
            try {
                LOGGER.debug("Completed send: {}", message.getJMSMessageID());
            } catch (JMSException e) {
            }
            completed.add(message);
            totalResult.add(message);
        }

        @Override
        public void onException(Message message, Exception exception) {
            try {
                LOGGER.debug("Failed send: {} -> error {}", message.getJMSMessageID(), exception.getMessage());
            } catch (JMSException e) {
            }
            failed.add(message);
            totalResult.add(message);
        }

        public List<Message> getCombinedSends() {
            return totalResult;
        }

        public List<Message> getCompletedSends() {
            return completed;
        }

        public List<Message> getFailedSends() {
            return failed;
        }
    }


}
