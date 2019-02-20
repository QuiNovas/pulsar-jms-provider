package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.config.PulsarDestination;

import javax.jms.JMSException;
import javax.jms.Queue;

public class PulsarQueue extends PulsarDestination implements Queue {

    public PulsarQueue() {
    }

    public PulsarQueue(String name) {
        super(name);
    }

    @Override
    public String getQueueName() throws JMSException {
        return getPhysicalName();
    }

}
