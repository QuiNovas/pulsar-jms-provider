package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.common.destination.PulsarDestinationMBean;
import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;
import com.echostreams.pulsar.jms.utils.Committable;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.Destination;
import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractDestination implements Destination, PulsarDestinationMBean, Committable {

    // Registered consumers
    protected List<PulsarMessageConsumer> localConsumers = new ArrayList<>();
    protected ReentrantReadWriteLock consumersLock = new ReentrantReadWriteLock(); // Protects localConsumers

    // Transaction handling
    protected ReentrantLock transactionLock = new ReentrantLock();

    // Runtime
    private long cumulativeCommitTime;
    private long commitCount;
    private long minCommitTime = Integer.MAX_VALUE;
    private long maxCommitTime = 0;
    protected boolean closed;
    protected Object closeLock = new Object();

    public void registerConsumer( PulsarMessageConsumer consumer )
    {
        consumersLock.writeLock().lock();
        try
        {
            localConsumers.add(consumer);
        }
        finally
        {
            consumersLock.writeLock().unlock();
        }
    }

    /**
     * Unregister a message listener
     */
    public void unregisterConsumer( PulsarMessageConsumer consumer )
    {
        consumersLock.writeLock().lock();
        try
        {
            localConsumers.remove(consumer);
        }
        finally
        {
            consumersLock.writeLock().unlock();
        }
    }

    public final boolean isClosed()
    {
        return closed;
    }

    protected final void checkNotClosed() throws JMSException
    {
        if (closed)
            throw new PulsarJMSException("Destination is closed","DESTINATION_IS_CLOSED");
    }

    protected final void checkTransactionLock() throws JMSException
    {
        if (requiresTransactionalUpdate() && !transactionLock.isHeldByCurrentThread())
            throw new PulsarJMSException("Destination is not locked for update","DESTINATION_NOT_LOCKED");
    }

    protected final PulsarMessageConsumer lookupConsumer( String consumerID )
    {
        consumersLock.readLock().lock();
        try
        {
            for (int i = 0; i < localConsumers.size(); i++)
            {
                PulsarMessageConsumer consumer = localConsumers.get(i);
                if (consumer.getSubscriberId().equals(consumerID))
                    return consumer;
            }
            return null;
        }
        finally
        {
            consumersLock.readLock().unlock();
        }
    }

    protected final boolean isConsumerRegistered( String consumerID )
    {
        return lookupConsumer(consumerID) != null;
    }


}
