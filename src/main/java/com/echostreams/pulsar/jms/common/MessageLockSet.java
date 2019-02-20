package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.queue.PulsarQueue;

import java.util.ArrayList;
import java.util.List;

public final class MessageLockSet
{
	private List<MessageLock> items;
    
    /**
     * Constructor
     */
    public MessageLockSet( int initialSize )
    {
        super();
        items = new ArrayList<>(initialSize);
    }
    
    /**
     * Add an handle to the list
     * @param handle
     */
    public void add( int handle , int deliveryMode , PulsarQueue destination , AbstractMessage message )
    {
    	items.add(new MessageLock(handle,deliveryMode,destination,message));
    }

    /**
     * Get the list size
     * @return the list size
     */
    public int size()
    {
    	return items.size();
    }
    
    /**
     * Get the nth item
     */
    public MessageLock get( int n )
    {
    	return items.get(n);
    }
}
