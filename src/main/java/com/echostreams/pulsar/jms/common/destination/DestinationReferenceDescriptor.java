package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.utils.Checkable;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;

/**
 * <p>Implementation of a destination reference descriptor.</p>
 */
public final class DestinationReferenceDescriptor implements Checkable
{
	private String destinationType;
	private String destinationName;
	
	public String getDestinationType()
	{
		return destinationType;
	}
	
	public void setDestinationType(String destinationType)
	{
		this.destinationType = destinationType;
	}
	
	public String getDestinationName()
	{
		return destinationName;
	}
	
	public void setDestinationName(String destinationName)
	{
		this.destinationName = destinationName;
	}

	@Override
	public void check() throws JMSException
	{
		if (destinationType == null)
			throw new PulsarJMSException("Missing destination reference property : 'destinationType'","INVALID_DESCRIPTOR");
		if (!destinationType.equals("queue") && !destinationType.equals("topic"))
			throw new PulsarJMSException("Destination reference property 'destinationType' should be one of : queue, topic","INVALID_DESCRIPTOR");
		if (destinationName == null)
			throw new PulsarJMSException("Missing destination reference property : 'destinationName'","INVALID_DESCRIPTOR");
	}

	@Override
	public String toString()
	{
		StringBuilder sb = new StringBuilder();
        
        sb.append("destinationType=");
        sb.append(destinationType);
        sb.append(" destinationName=");
        sb.append(destinationName);
        
        return sb.toString();
	}
}
