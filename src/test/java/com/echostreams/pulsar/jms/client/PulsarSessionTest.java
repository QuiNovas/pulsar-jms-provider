package com.echostreams.pulsar.jms.client;

import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

import static org.junit.Assert.*;

public class PulsarSessionTest extends PulsarConnectionTestSupport {

    private static final int NO_ACKNOWLEDGE = 257;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockPulsarClient();
    }

    @Test(timeout = 10000)
    public void testGetMessageListener() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNull(session.getMessageListener());
        session.setMessageListener(new MessageListener() {

            @Override
            public void onMessage(Message message) {
            }
        });
        assertNotNull(session.getMessageListener());
    }

    @Test(timeout = 10000)
    public void testGetAcknowledgementMode() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertEquals(Session.AUTO_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (PulsarSession) connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        assertEquals(Session.CLIENT_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (PulsarSession) connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        assertEquals(Session.DUPS_OK_ACKNOWLEDGE, session.getAcknowledgeMode());
        session = (PulsarSession) connection.createSession(true, Session.SESSION_TRANSACTED);
        assertEquals(Session.SESSION_TRANSACTED, session.getAcknowledgeMode());
        session = (PulsarSession) connection.createSession(false, NO_ACKNOWLEDGE);
        assertEquals(NO_ACKNOWLEDGE, session.getAcknowledgeMode());
    }

    @Test(timeout = 10000)
    public void testCreateMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMessage());
    }

    @Test(timeout = 10000)
    public void testCreateBytesMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createBytesMessage());
    }

    @Test(timeout = 10000)
    public void testCreateStreamMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createStreamMessage());
    }

    @Test(timeout = 10000)
    public void testCreateMapMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createMapMessage());
    }

    @Test(timeout = 10000)
    public void testCreateObjectMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createObjectMessage());
    }

    @Test(timeout = 10000)
    public void testCreateObjectMessageWithValue() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ObjectMessage message = session.createObjectMessage("TEST-OBJ-MESSAGE");
        assertNotNull(message);
        assertNotNull(message.getObject());
        assertTrue(message.getObject() instanceof String);
        assertEquals("TEST-OBJ-MESSAGE", message.getObject());
    }

    @Test(timeout = 10000)
    public void testCreateTextMessage() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        assertNotNull(session.createTextMessage());
    }

    @Test(timeout = 10000)
    public void testCreateTextMessageWithValue() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        TextMessage message = session.createTextMessage("TEST-TEXT-MESSAGE");
        assertNotNull(message);
        assertEquals("TEST-TEXT-MESSAGE", message.getText());
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreateProducerNullDestinationthrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(null);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreatePublisherNullDestinationthrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createPublisher(null);
    }

    @Test(timeout = 10000, expected = NullPointerException.class)
    public void testUnsubscribeNullConsumerThrowsNPE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.unsubscribe("test-subscription");
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreateConsumerNullDestinationThrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createConsumer(null);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreateReceiverNullDestinationThrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createReceiver(null);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreateSubscriberNullDestinationThrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createSubscriber(null);
    }

    @Test(timeout = 10000, expected = IllegalArgumentException.class)
    public void testCreateSubscriberNullDestinationWithSelectorNoLocalThrowsIAE() throws JMSException {
        PulsarSession session = (PulsarSession) connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createSubscriber(null, "a > b", true);
    }

}
