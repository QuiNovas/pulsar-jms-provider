package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.common.AbstractSession;

import javax.jms.*;

public class PulsarQueueSession extends AbstractSession implements QueueSession {

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return null;
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String s) throws JMSException {
        return null;
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return null;
    }
}
