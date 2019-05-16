package com.echostreams.pulsar.jms.client;

import org.junit.Test;

import javax.jms.JMSException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class PulsarTopicTest {

    @Test
    public void testGetName() throws JMSException {
        PulsarTopic topic = new PulsarTopic("myTopic");
        assertEquals("myTopic", topic.getName());
    }

    @Test
    public void testNullName() throws Exception {
        PulsarTopic topic = new PulsarTopic(null);
        assertNull(topic.getName());
    }
}
