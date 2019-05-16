package com.echostreams.pulsar.jms.client;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;

import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyLong;

public class PulsarMessageConsumerTest extends PulsarConnectionTestSupport {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageConsumerTest.class);

    @Test
    public void testGetMessageSelector() throws JMSException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.getMessageSelector());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).getMessageSelector();
    }

    @Test
    public void testGetMessageListener() throws JMSException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.getMessageListener());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).getMessageListener();
    }

    @Test
    public void testReceivePassThrough() throws JMSException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.receive());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receive();
    }

    @Test
    public void testTimedReceivePassThrough() throws JMSException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.receive(100));
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receive(anyLong());
    }

    @Test
    public void testReceiveNoWaitPassThrough() throws JMSException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.receiveNoWait());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveNoWait();
    }

    @Test
    public void testReceiveAsyncPassThrough() throws JMSException, ExecutionException, InterruptedException {
        PulsarMessageConsumer consumer = Mockito.mock(PulsarMessageConsumer.class);

        try {
            assertNull(consumer.receiveAsync());
        } finally {
            consumer.close();
        }

        Mockito.verify(consumer, Mockito.times(1)).receiveAsync();
    }
}
