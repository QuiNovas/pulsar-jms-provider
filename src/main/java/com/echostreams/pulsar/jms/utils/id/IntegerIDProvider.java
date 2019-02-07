package com.echostreams.pulsar.jms.utils.id;

public final class IntegerIDProvider
{
	// Runtime
    private int nextId = 1;

    public IntegerIDProvider()
    {
        super();
    }

    public synchronized IntegerID createID()
    {
    	return new IntegerID(nextId++);
    }
}
