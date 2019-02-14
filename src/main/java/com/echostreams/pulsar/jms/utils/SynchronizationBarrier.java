package com.echostreams.pulsar.jms.utils;

public final class SynchronizationBarrier
{
    private boolean reached;
    private int remaining;
    
    /**
     * Increment the number of involved parties
     */
    public synchronized void addParty()
    {
        remaining++;
    }
    
    /**
     * Test if the synchronization point was reached
     */
    public synchronized boolean isReached()
    {
        return reached;
    }
    
    /**
     * Wait for the synchronization point to be reached.
     * Returns immediately if the sync. point is already reached.
     */
    public synchronized void waitFor() throws InterruptedException
    {
    	if (remaining == 0)
    		reached = true;
    	while (!reached)
    	    wait();
    }
    
    /**
     * Indicate that the current thread reached the synchronization barrier.
     * If all parties have reached the barrier, this wakes up all waiting threads.
     */
    public synchronized void reach()
    {
        if (reached)
            return;
        
        remaining--;
        if (remaining <= 0)
        {
            reached = true;
            notifyAll();
        }
    }
    
    /**
     * Reset the synchronization barrier
     */
    public synchronized void reset()
    {
        remaining = 0;
        reached = false;
    }
}
