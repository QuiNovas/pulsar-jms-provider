package com.echostreams.pulsar.jms.client;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import java.util.UUID;

import static org.junit.Assert.*;

public class PulsarJMSContextTest extends PulsarConnectionTestSupport {

    private PulsarJMSContext context;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        context = createContextToMockPulsarClient();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        if (context != null) {
            context.close();
        }
    }

    @Test
    public void testCreateContextWithNewAcknowledgementMode() {
        JMSContext newContext = context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);

        assertNotNull(newContext);
        assertEquals(JMSContext.CLIENT_ACKNOWLEDGE, newContext.getSessionMode());
        newContext.close();

    }

    @Test
    public void testGetMeataData() {
        assertNotNull(context.getMetaData());
    }

    @Test
    public void testCreateContextFromClosedContextThrowsISRE() {
        context.close();
        context.createContext(JMSContext.CLIENT_ACKNOWLEDGE);
    }

    @Test
    public void testCreateTextMessage() throws JMSException {
        TextMessage message = context.createTextMessage();
        assertNotNull(message);
        assertNull(message.getText());
    }

    @Test
    public void testCreateTextMessageWithBody() throws JMSException {
        TextMessage message = context.createTextMessage("test");
        assertNotNull(message);
        assertEquals("test", message.getText());
    }

    @Test
    public void testCreateBytesMessage() throws JMSException {
        BytesMessage message = context.createBytesMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateMapMessage() throws JMSException {
        MapMessage message = context.createMapMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateMessage() throws JMSException {
        Message message = context.createMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateStreamMessage() throws JMSException {
        StreamMessage message = context.createStreamMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateObjectMessage() throws JMSException {
        ObjectMessage message = context.createObjectMessage();
        assertNotNull(message);
    }

    @Test
    public void testCreateObjectMessageWithBody() throws JMSException {
        UUID payload = UUID.randomUUID();
        ObjectMessage message = context.createObjectMessage(payload);
        assertNotNull(message);
        assertEquals(payload, message.getObject());
    }

    @Test
    public void testCreateTopicPassThrough() throws JMSException {
        try {
            assertNotNull(context.createTopic("test"));
        } finally {
            context.close();
        }
    }

    @Test
    public void testCreateQueuePassthrough() throws JMSException {
        try {
            assertNotNull(context.createQueue("test"));
        } finally {
            context.close();
        }
    }

}
