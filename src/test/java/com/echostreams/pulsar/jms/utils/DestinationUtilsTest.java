package com.echostreams.pulsar.jms.utils;

import com.echostreams.pulsar.jms.client.PulsarDestination;
import com.echostreams.pulsar.jms.client.PulsarQueue;
import com.echostreams.pulsar.jms.client.PulsarTopic;
import org.junit.Test;

import javax.jms.JMSException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class DestinationUtilsTest {

    @Test
    public void testTransformDestination() throws JMSException {
        PulsarDestination destination = null;

        assertNull(DestinationUtils.transformDestination(destination));

        destination = new PulsarQueue("pulsarQueue");
        destination = DestinationUtils.transformDestination(destination);
        assertNotNull(destination);
        assertEquals("pulsarQueue", destination.getName());

        destination = new PulsarTopic("pulsarTopic");
        destination = DestinationUtils.transformDestination(destination);
        assertNotNull(destination);
        assertEquals("pulsarTopic", destination.getName());
    }
}
