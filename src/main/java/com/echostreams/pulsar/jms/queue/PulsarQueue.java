package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.common.AbstractDestination;

import javax.jms.JMSException;
import javax.jms.Queue;

public class PulsarQueue extends AbstractDestination implements Queue {

    @Override
    public String getQueueName() throws JMSException {
        return null;
    }
}
