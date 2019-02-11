package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.config.PulsarConfiguration;

import javax.jms.Queue;
import javax.naming.NamingException;
import javax.naming.Reference;

public class QueueRef extends DestinationRef implements Queue
{
	private static final long serialVersionUID = 1L;
	
	// Attributes
	protected String name;
    
    /**
     * Constructor
     */
    public QueueRef( String name )
    {
        this.name = name;
    }

    @Override
	public final String getQueueName()
    {
        return name;
    }

    @Override
	public final String getResourceName() 
    {
		return PulsarConfiguration.QUEUE_PREFIX+name;
	}

    @Override
	public final Reference getReference() throws NamingException
    {
    	/*Reference ref = new Reference(getClass().getName(),JNDIObjectFactory.class.getName(),null);
    	ref.add(new StringRefAddr("queueName",name));
    	return ref;*/
        return null;
    }

    @Override
	public String toString()
    {
        return "Queue("+name+")";
    }
}
