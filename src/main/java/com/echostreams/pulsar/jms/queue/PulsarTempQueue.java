package com.echostreams.pulsar.jms.queue;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

public class PulsarTempQueue extends PulsarQueue implements TemporaryQueue {
    @Override
    public void delete() throws JMSException {

    }
}
