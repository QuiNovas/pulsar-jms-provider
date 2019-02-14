package com.echostreams.pulsar.jms.utils;

import javax.jms.JMSException;

public interface Committable
{
	/**
     * Get the committable name
     */
    public String getName();
    
	/**
     * Lock the destination for transactional update
     */
    public void openTransaction();

    /**
     * Unlock the destination for transactional update
     */
    public void closeTransaction();
    
    /**
     * Commit all changes (make sure they are really applied)
     */
    public void commitChanges() throws JMSException;
    
	/**
     * Commit all changes (make sure they are really applied) (ASYNCHRONOUS)
     * @param barrier a synchronization barrier
     */
    public void commitChanges(SynchronizationBarrier barrier) throws JMSException;
}
