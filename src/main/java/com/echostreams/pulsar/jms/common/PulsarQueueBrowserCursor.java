package com.echostreams.pulsar.jms.common;

public final class PulsarQueueBrowserCursor
{
    // Attributes
	private int position = 0;
	private int skipped = 0;
	private boolean endOfQueueReached = false;
	
	public boolean endOfQueueReached()
	{
		return endOfQueueReached;
	}
	
	public void setEndOfQueueReached()
	{
		endOfQueueReached = true;
	}
	
	public int position()
	{
		return position;
	}
	
	public int skipped()
	{
		return skipped;
	}
	
	public void skip()
	{
		skipped++;
	}
	
	public void move()
	{
		position = skipped+1;
		skipped = 0;
	}
	
	public void reset()
	{
		skipped = 0;
	}
}
