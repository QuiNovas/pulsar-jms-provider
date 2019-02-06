package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.message.PulsarMessageProducer;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.QueueSender;

public class PulsarQueueSender extends PulsarMessageProducer implements QueueSender {

    @Override
    public Queue getQueue() throws JMSException {
        return null;
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {

    }

    @Override
    public void send(Queue queue, Message message, int i, int i1, long l) throws JMSException {

    }
}
