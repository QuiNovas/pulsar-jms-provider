package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.common.AbstractConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;
import javax.jms.TemporaryQueue;

public final class TemporaryQueueRef extends QueueRef implements TemporaryQueue, TemporaryDestination
{
	private static final long serialVersionUID = 1L;
	
	// Reference to the parent connection
    private transient AbstractConnection connection;
    
    /**
     * Constructor
     */
    public TemporaryQueueRef( AbstractConnection connection , String queueName )
    {
        super(queueName);
        this.connection = connection;
    }
    
    /*
     * (non-Javadoc)
     * @see javax.jms.TemporaryQueue#delete()
     */
    @Override
	public void delete() throws JMSException
    {
        if (connection == null)
            throw new PulsarJMSException("Temporary queue already deleted","QUEUE_DOES_NOT_EXIST");
            
        connection.deleteTemporaryQueue(name);
        connection = null;
    }

    /*
     * (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    @Override
	public String toString()
    {
        return super.toString()+"[T]";
    }
}
