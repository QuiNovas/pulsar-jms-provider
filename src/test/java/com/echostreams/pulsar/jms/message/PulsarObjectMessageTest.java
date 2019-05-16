package com.echostreams.pulsar.jms.message;

import org.junit.Test;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import java.io.IOException;

import static org.junit.Assert.*;
import static org.junit.Assert.fail;

public class PulsarObjectMessageTest {

    @Test
    public void testBytes() throws JMSException, IOException {
        PulsarObjectMessage msg;

        msg = new PulsarObjectMessage();
        String str = "testText";
        msg.setObject(str);

        assertEquals(msg.getObject(), str);
    }

    @Test
    public void testSetObject() throws JMSException {
        PulsarObjectMessage objectMessage;

        objectMessage = new PulsarObjectMessage();
        String str = "testText";
        objectMessage.setObject(str);
        assertEquals(str, objectMessage.getObject());
    }

    @Test
    public void testClearBody() throws JMSException {
        PulsarObjectMessage objectMessage;

        objectMessage = new PulsarObjectMessage();
        try {
            objectMessage.setObject("String");
            objectMessage.clearBody();
            assertFalse(objectMessage.readOnlyBody);
            assertNull(objectMessage.getObject());
            objectMessage.setObject("String");
            objectMessage.getObject();
        } catch (MessageNotWriteableException mnwe) {
            fail("should be writeable");
        }
    }

    @Test
    public void testToString() throws Exception {
        PulsarObjectMessage objectMessage;

        objectMessage = new PulsarObjectMessage();
        assertTrue(objectMessage.toString().startsWith("PulsarObjectMessage"));
    }
}
