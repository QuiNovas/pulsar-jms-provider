package com.echostreams.pulsar.jms.common;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.Enumeration;

public abstract class AbstractQueueBrowser implements QueueBrowser {

    @Override
    public Queue getQueue() throws JMSException {
        return null;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return null;
    }

    @Override
    public Enumeration getEnumeration() throws JMSException {
        return null;
    }

    @Override
    public void close() throws JMSException {

    }
}
