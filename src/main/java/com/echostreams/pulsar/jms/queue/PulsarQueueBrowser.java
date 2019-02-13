package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.common.AbstractQueueBrowser;
import com.echostreams.pulsar.jms.common.AbstractSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.UUIDProvider;

import javax.jms.JMSException;
import javax.jms.Queue;
import java.util.Enumeration;

public class PulsarQueueBrowser extends AbstractQueueBrowser {

    private String messageSelector;

    public PulsarQueueBrowser(AbstractSession session, Queue queue, String messageSelector, IntegerID browserId) {
        super(session, queue, messageSelector, browserId);
    }

    @Override
    public Enumeration getEnumeration() throws JMSException
    {
        checkNotClosed();

        PulsarQueueBrowserEnumeration queueBrowserEnum = new PulsarQueueBrowserEnumeration(this,(PulsarQueue)queue,messageSelector, UUIDProvider.getInstance().getShortUUID());
        registerEnumeration(queueBrowserEnum);
        return queueBrowserEnum;
    }
}
