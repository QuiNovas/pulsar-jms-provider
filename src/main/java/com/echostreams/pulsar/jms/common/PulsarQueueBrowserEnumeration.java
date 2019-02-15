package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.queue.PulsarQueue;
import com.echostreams.pulsar.jms.queue.PulsarQueueBrowser;
import javax.jms.JMSException;
import javax.jms.QueueBrowser;
import java.util.NoSuchElementException;

public final class PulsarQueueBrowserEnumeration extends AbstractQueueBrowserEnumeration
{
	private PulsarQueue pulsarQueue;
	private String messageSelector;
	private PulsarQueueBrowserCursor cursor = new PulsarQueueBrowserCursor();

	// Runtime
	private AbstractMessage nextMessage;

	public PulsarQueueBrowserEnumeration(PulsarQueueBrowser browser, PulsarQueue pulsarQueue, String messageSelector, String enumId)
	{
		super(browser,enumId);
		this.pulsarQueue = pulsarQueue;
		this.messageSelector = messageSelector;
	}

	private AbstractMessage fetchNext() throws JMSException
	{
		if (nextMessage != null)
			return nextMessage; // Already fetched
		nextMessage = pulsarQueue.browse(cursor, messageSelector); // Lookup next candidate
		if (nextMessage == null)
			close(); // Auto-close enumeration at end of queue
		return nextMessage;
	}

	@Override
	public boolean hasMoreElements()
	{
		if (cursor.endOfQueueReached())
			return false;
		
		try
		{
			checkNotClosed();
			AbstractMessage msg = fetchNext();
			return msg != null;
		}
		catch (JMSException e)
		{
			throw new IllegalStateException(e.toString());
		}
	}

	@Override
	public AbstractMessage nextElement()
	{
		if (cursor.endOfQueueReached())
			throw new NoSuchElementException();
		
		try
		{
			checkNotClosed();
			AbstractMessage msg = fetchNext();
			if (msg != null)
			{
				nextMessage = null; // Consume fetched message
				AbstractMessage msgCopy = MessageTools.duplicate(msg);
				msgCopy.ensureDeserializationLevel(MessageSerializationLevel.FULL);
				return msgCopy;
			}
			
			throw new NoSuchElementException();
		}
		catch (NoSuchElementException e)
		{
			throw e;
		}
		catch (JMSException e)
		{
			throw new IllegalStateException(e.toString());
		}
	}
}
