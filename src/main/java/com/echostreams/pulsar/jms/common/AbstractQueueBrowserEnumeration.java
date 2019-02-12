package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.JavaRelatedUtils;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;
import java.util.Enumeration;

public abstract class AbstractQueueBrowserEnumeration implements Enumeration<AbstractMessage>
{
    // Unique ID
    protected String id;
    
    // Parent browser
    protected AbstractQueueBrowser browser;
    
    // Attributes
    protected Object closeLock = new Object();
    protected boolean closed;

	public AbstractQueueBrowserEnumeration(AbstractQueueBrowser browser, String enumId)
	{
		this.id = enumId;
		this.browser = browser;
	}

	public final String getId()
	{
		return id;
	}

	public final void checkNotClosed() throws JMSException
	{
		if (closed)
			throw new PulsarJMSException("Queue browser enumeration is closed","ENUMERATION_CLOSED");
	}
	
	/**
	 * Close the enumeration
	 */
	public final void close()
	{
		synchronized (closeLock)
		{
			if (closed)
				return;
			closed = true;
			onQueueBrowserEnumerationClose();
		}
	}
	
	protected void onQueueBrowserEnumerationClose()
	{
		browser.unregisterEnumeration(this);
	}
	
    @Override
	public String toString()
    {
    	StringBuilder sb = new StringBuilder();
        
        sb.append(JavaRelatedUtils.getShortClassName(getClass()));
        sb.append("[#");
        sb.append(id);
        sb.append("]");
        
        return sb.toString();
    }
}
