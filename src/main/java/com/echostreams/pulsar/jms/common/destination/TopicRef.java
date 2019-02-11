package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.config.PulsarConfiguration;

import javax.jms.Topic;
import javax.naming.NamingException;
import javax.naming.Reference;

public class TopicRef extends DestinationRef implements Topic
{
	private static final long serialVersionUID = 1L;
	
	// Attributes
    protected String name;

    public TopicRef( String topicName )
    {
        this.name = topicName;
    }

    @Override
	public final String getTopicName()
    {
        return name;
    }

    @Override
	public final String getResourceName() 
    {
		return PulsarConfiguration.TOPIC_PREFIX+name;
	}

    @Override
	public final Reference getReference() throws NamingException
    {
    	/*Reference ref = new Reference(getClass().getName(),JNDIObjectFactory.class.getName(),null);
    	ref.add(new StringRefAddr("topicName",name));
    	return ref;*/
        return null;
    }

    @Override
	public String toString()
    {
        return "Topic("+name+")";
    }
}
