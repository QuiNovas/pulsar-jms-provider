package com.echostreams.pulsar.jms.message;

import org.junit.Test;

import javax.jms.JMSException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PulsarTextMessageTest {

    @Test
    public void testSetText() throws JMSException {
        PulsarTextMessage textMessage;

        textMessage = new PulsarTextMessage();
        String str = "testText";
        textMessage.setText(str);
        assertEquals(textMessage.getText(), str);
    }

    @Test
    public void testClearBody() throws JMSException, IOException {
        PulsarTextMessage textMessage;

        textMessage = new PulsarTextMessage();
        textMessage.setText("string");
        textMessage.clearBody();
        assertNull(textMessage.getText());
    }

    @Test
    public void testNullText() throws Exception {
        PulsarTextMessage textMessage;

        textMessage = new PulsarTextMessage();
        textMessage.setText(null);
        assertNull(textMessage.getText());
    }

    @Test
    public void testToString() throws Exception {
        PulsarTextMessage textMessage;

        textMessage = new PulsarTextMessage();
        assertTrue(textMessage.toString().startsWith("PulsarTextMessage"));
    }

}
