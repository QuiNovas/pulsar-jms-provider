package com.echostreams.pulsar.jms.common.destination;

public interface PulsarDestinationMBean
{
    /**
     * Reset statistics on this destination
     */
    public void resetStats();
    
    /**
     * Get the destination size (number of contained messages)
     */
    public int getSize();
    
    /**
     * Get the number of currently registered consumers on this destination
     */
    public int getRegisteredConsumersCount();
    
    /**
     * Get the minimum commit time for this queue (milliseconds)
     */
	public long getMinCommitTime();
	
	/**
     * Get the maximum commit time for this queue (milliseconds)
     */
	public long getMaxCommitTime();
    
    /**
     * Get the average commit time for this queue (milliseconds)
     */
    public double getAverageCommitTime();
}
