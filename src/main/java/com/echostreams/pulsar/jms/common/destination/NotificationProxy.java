package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

/**
 * <p>
 *  Interface for a notification proxy. 
 *  A notification proxy propagates message availability events to upper layers of the implementation.
 * </p>
 */
public interface NotificationProxy
{
	/**
     * Send a notification packet through this proxy
     */
    public void addNotification(IntegerID consumerId, AbstractMessage prefetchedMessage);
    
    /**
     * Flush buffered notifications
     */
    public void flush();
}
