package com.echostreams.pulsar.jms.client;

import org.junit.Test;

import javax.jms.JMSException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PulsarQueueTest {

    @Test
    public void testGetName() throws JMSException {
        PulsarQueue queue = new PulsarQueue("myQueue");
        assertEquals("myQueue", queue.getName());
    }

    @Test
    public void testNullName() throws Exception {
        PulsarQueue queue = new PulsarQueue(null);
        assertNull(queue.getName());
    }
}
