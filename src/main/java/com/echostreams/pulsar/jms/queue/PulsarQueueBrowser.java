package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.common.AbstractQueueBrowser;

import javax.jms.JMSException;
import java.util.Enumeration;

public class PulsarQueueBrowser extends AbstractQueueBrowser {

    private MessageSelector parsedSelector;

    @Override
    public Enumeration getEnumeration() throws JMSException {
        return null;
    }
}
