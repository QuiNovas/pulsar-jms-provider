package com.echostreams.pulsar.jms.client;

import javax.jms.JMSException;
import javax.jms.Queue;

public class PulsarQueue implements Queue, PulsarDestination {
    private static final long serialVersionUID = -7830091263426455391L;

    private String queueName;

    public PulsarQueue(String queueName) {
        this.queueName = queueName;
    }

    @Override
    public String getQueueName() throws JMSException {
        return queueName;
    }

    @Override
    public String getName() throws JMSException {
        return getQueueName();
    }

}
