package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import java.util.*;

public abstract class AbstractQueueBrowser implements QueueBrowser {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractQueueBrowser.class);

    // Unique ID
    protected IntegerID id;

    // Attributes
    protected Queue queue;
    protected String messageSelector;

    // Runtime
    protected Object closeLock = new Object();
    protected boolean closed;

    // Parent session
    protected AbstractSession session;

    // Children
    private Map<String,AbstractQueueBrowserEnumeration> enumMap = new Hashtable<>();

    public AbstractQueueBrowser( AbstractSession session , Queue queue , String messageSelector , IntegerID browserId )
    {
        this.session = session;
        this.queue = queue;
        this.messageSelector = messageSelector;
        this.id = browserId;
    }


    @Override
    public Queue getQueue() throws JMSException {
        return queue;
    }

    @Override
    public String getMessageSelector() throws JMSException {
        return messageSelector;
    }

    @Override
    public void close() throws JMSException {
        synchronized (closeLock)
        {
            if (closed)
                return;
            this.closed = true;
            onQueueBrowserClose();
        }
    }

    public final IntegerID getId()
    {
        return id;
    }

    protected final void registerEnumeration( AbstractQueueBrowserEnumeration queueBrowserEnum )
    {
        enumMap.put(queueBrowserEnum.getId(),queueBrowserEnum);
    }

    protected final void unregisterEnumeration( AbstractQueueBrowserEnumeration queueBrowserEnum )
    {
        enumMap.put(queueBrowserEnum.getId(),queueBrowserEnum);
    }

    public final AbstractQueueBrowserEnumeration lookupRegisteredEnumeration( String enumId )
    {
        return enumMap.get(enumId);
    }

    private void closeRemainingEnumerations()
    {
        List<AbstractQueueBrowserEnumeration> enumsToClose = new Vector<>();
        synchronized (enumMap)
        {
            enumsToClose.addAll(enumMap.values());
            for (int n = 0 ; n < enumsToClose.size() ; n++)
            {
                AbstractQueueBrowserEnumeration queueBrowserEnum = enumsToClose.get(n);
                LOGGER.debug("Auto-closing unclosed queue browser enumeration : "+queueBrowserEnum);
                try
                {
                    queueBrowserEnum.close();
                }
                catch (Exception e)
                {
                    LOGGER.error("Could not close queue browser enumeration "+queueBrowserEnum,e);
                }
            }
        }
    }

    protected void onQueueBrowserClose()
    {
        session.unregisterBrowser(this);
        closeRemainingEnumerations();
    }

    public final void checkNotClosed() throws JMSException
    {
        if (closed)
            throw new PulsarJMSException("Queue browser is closed","QUEUE_BROWSER_CLOSED");
    }

}
