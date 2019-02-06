package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

public class PulsarQueueReceiver extends PulsarMessageConsumer implements QueueReceiver {

    @Override
    public Queue getQueue() throws JMSException {
        return null;
    }
}
