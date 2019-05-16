package com.echostreams.pulsar.jms.client;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;

public class PulsarJMSConsumerTest extends PulsarConnectionTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSConsumerTest.class);

    @Test
    public void testGetMessageSelector() throws JMSException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.getMessageSelector());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).getMessageSelector();
    }

    @Test
    public void testGetMessageListener() throws JMSException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.getMessageListener());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).getMessageListener();
    }

    @Test
    public void testReceivePassThrough() throws JMSException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receive());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receive();
    }

    @Test
    public void testTimedReceivePassThrough() throws JMSException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receive(100));
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receive(anyLong());
    }

    @Test
    public void testReceiveNoWaitPassThrough() throws JMSException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receiveNoWait());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveNoWait();
    }

    @Test
    public void testReceiveBodyPassThrough() throws JMSException, ExecutionException, InterruptedException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receiveBody(Map.class));
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveBody(Map.class);
    }

    @Test
    public void testTimedReceiveBodyPassThrough() throws JMSException, ExecutionException, InterruptedException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receiveBody(Map.class, 100));
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveBody(Map.class, 100);
    }

    @Test
    public void testReceiveBodyNoWaitPassThrough() throws JMSException, ExecutionException, InterruptedException {
        PulsarJMSConsumer consumer = Mockito.mock(PulsarJMSConsumer.class);

        try {
            assertNull(consumer.receiveBodyNoWait(Map.class));
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveBodyNoWait(Map.class);
    }
}
