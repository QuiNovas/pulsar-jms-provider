package com.echostreams.pulsar.jms.common.destination;


import com.echostreams.pulsar.jms.common.PulsarSubscriberPolicy;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.Settings;
import com.echostreams.pulsar.jms.utils.StringRelatedUtils;

import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import java.util.StringTokenizer;

public final class TopicDefinition extends AbstractDestinationDefinition
{
	// Attributes
	private int subscriberFailurePolicy;
	private int subscriberOverflowPolicy;
	private String[] partitionsKeysToIndex;	
	
    /**
     * Constructor
     */
    public TopicDefinition()
    {
        super();
    }
    
    /**
     * Constructor
     */
    public TopicDefinition( Settings settings )
    {
        super(settings);
    }

	public void setSubscriberFailurePolicy(int subscriberFailurePolicy)
	{
		this.subscriberFailurePolicy = subscriberFailurePolicy;
	}

	public void setSubscriberOverflowPolicy(int subscriberOverflowPolicy)
	{
		this.subscriberOverflowPolicy = subscriberOverflowPolicy;
	}

	public int getSubscriberFailurePolicy()
	{
		return subscriberFailurePolicy;
	}

	public int getSubscriberOverflowPolicy()
	{
		return subscriberOverflowPolicy;
	}

	public void setPartitionsKeysToIndex(String[] partitionsKeysToIndex)
	{
		this.partitionsKeysToIndex = partitionsKeysToIndex;
	}

	public String[] getPartitionsKeysToIndex()
	{
		return partitionsKeysToIndex;
	}

	@Override
	protected void initFromSettings(Settings settings)
	{
		super.initFromSettings(settings);
		
		this.subscriberFailurePolicy  = settings.getIntProperty("subscriberFailurePolicy",PulsarSubscriberPolicy.SUBSCRIBER_POLICY_LOG);
		this.subscriberOverflowPolicy = settings.getIntProperty("subscriberOverflowPolicy", PulsarSubscriberPolicy.SUBSCRIBER_POLICY_LOG);
		
		String rawPartitionsKeysToIndex = settings.getStringProperty("partitionsKeysToIndex");
		if (rawPartitionsKeysToIndex != null)
		{
			StringTokenizer st = new StringTokenizer(rawPartitionsKeysToIndex, ", ");
			this.partitionsKeysToIndex = new String[st.countTokens()];
			int pos = 0;
			while (st.hasMoreTokens())
				this.partitionsKeysToIndex[pos++] = st.nextToken();
		}
	}

	@Override
	protected void fillSettings(Settings settings)
	{
		super.fillSettings(settings);
		
		settings.setIntProperty("subscriberFailurePolicy", subscriberFailurePolicy);
		settings.setIntProperty("subscriberOverflowPolicy", subscriberOverflowPolicy);
		if (partitionsKeysToIndex != null)
			settings.setStringProperty("partitionsKeysToIndex", StringRelatedUtils.implode(partitionsKeysToIndex, ","));
	}
	
    /**
     * Create a queue definition from this template
     */
    public QueueDefinition createQueueDefinition( String topicName , String consumerId , boolean temporary )
    {
        QueueDefinition def = new QueueDefinition();
        def.setName(DestinationTools.getQueueNameForTopicConsumer(topicName, consumerId));
        def.setTemporary(temporary);
        copyAttributesTo(def);
        
        return def;
    }

    public boolean supportDeliveryMode( int deliveryMode )
    {
    	switch (deliveryMode)
    	{
    		case DeliveryMode.PERSISTENT :     return initialBlockCount > 0;
    		case DeliveryMode.NON_PERSISTENT : return maxNonPersistentMessages > 0;
    		default :
    			throw new IllegalArgumentException("Invalid delivery mode : "+deliveryMode);
    	}
    }

    /*
     * (non-Javadoc)
     * @see net.timewalker.ffmq4.management.destination.DestinationDescriptor#check()
     */
    @Override
	public void check() throws JMSException
    {
        super.check();
        
        // Check queue name
        DestinationTools.checkTopicName(name);
        
        // Policies
        if (!PulsarSubscriberPolicy.isValid(subscriberFailurePolicy))
    		throw new PulsarJMSException("Invalid subscriber failure policy mask : "+subscriberFailurePolicy,"INVALID_DESCRIPTOR");
    	if (!PulsarSubscriberPolicy.isValid(subscriberOverflowPolicy))
    		throw new PulsarJMSException("Invalid subscriber overflow policy mask : "+subscriberOverflowPolicy,"INVALID_DESCRIPTOR");
    	
    	// Partition keys
    	if (partitionsKeysToIndex != null)
    	{
    		if (partitionsKeysToIndex.length == 0)
    			throw new PulsarJMSException("Empty partitionsKeysToIndex definition","INVALID_DESCRIPTOR");
    		
    		for(String key : partitionsKeysToIndex)
    		{
    			if (key.startsWith("JMS") && !key.equals("JMSCorrelationID"))
    				throw new PulsarJMSException("JMSCorrelationID is the only JMS standard header that may be indexed, cannot use "+key,"INVALID_DESCRIPTOR");
    		}
    	}
    }
}
