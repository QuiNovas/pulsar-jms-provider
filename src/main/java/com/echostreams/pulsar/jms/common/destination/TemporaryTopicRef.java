package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.common.AbstractConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;
import javax.jms.TemporaryTopic;

public final class TemporaryTopicRef extends TopicRef implements TemporaryTopic, TemporaryDestination
{
	private static final long serialVersionUID = 1L;
	
	// Reference to the parent connection
	private transient AbstractConnection connection;

    public TemporaryTopicRef( AbstractConnection connection , String topicName )
    {
        super(topicName);
        this.connection = connection;
    }

    @Override
	public void delete() throws JMSException
    {
        if (connection == null)
            throw new PulsarJMSException("Temporary topic already deleted","TOPIC_DOES_NOT_EXIST");
        
        connection.deleteTemporaryTopic(name);
        connection = null;
    }

    @Override
	public String toString()
    {
        return super.toString()+"[T]";
    }
}
