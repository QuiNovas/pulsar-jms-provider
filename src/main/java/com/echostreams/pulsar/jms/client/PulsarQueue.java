package com.echostreams.pulsar.jms.client;

import javax.jms.JMSException;
import javax.jms.Queue;

public class PulsarQueue implements Queue, PulsarDestination {
    private String queueName;

    public PulsarQueue(String queueName) {
        this.queueName = queueName;
    }

    /* (non-Javadoc)
     * @see javax.jms.Queue#getQueueName()
     */
    @Override
    public String getQueueName() throws JMSException {
        return queueName;
    }

    /* (non-Javadoc)
     * @see PulsarDestination#getName()
     */
    @Override
    public String getName() throws JMSException {
        return getQueueName();
    }

}
