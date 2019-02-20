package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.queue.PulsarQueue;

import javax.jms.Session;

/**
 * <p>Item of a {@link TransactionSet}. Keeps track of a message lock obtained by a JMS {@link Session} in the current transaction.</p>
 */
public final class TransactionItem
{
	// Attributes
	private int handle;
	private String messageId;
	private int deliveryMode;
	private PulsarQueue destination;
	
	/**
	 * Constructor
	 */
	public TransactionItem( int handle,
	                        String messageID,
	                        int deliveryMode,
							PulsarQueue destination )
	{
		this.handle = handle;
		this.messageId = messageID;
		this.deliveryMode = deliveryMode;
		this.destination = destination;
	}

	public int getHandle()
	{
		return handle;
	}
	
	/**
	 * @param handle the handle to set
	 */
	public void setHandle(int handle)
	{
		this.handle = handle;
	}
	
	public String getMessageId()
	{
		return messageId;
	}

	public int getDeliveryMode()
	{
		return deliveryMode;
	}

	public PulsarQueue getDestination()
	{
		return destination;
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
		sb.append("[TransactionItem] handle=");
		sb.append(handle);
		sb.append(" messageID=");
		sb.append(messageId);
		sb.append(" destination=");
		sb.append(destination);
		sb.append(" deliveryMode=");
		sb.append(deliveryMode);
		
		return sb.toString();
	}
	
	public MessageLock toMessageLock( AbstractMessage message )
	{
		return new MessageLock(handle, deliveryMode, destination, message);
	}
}
