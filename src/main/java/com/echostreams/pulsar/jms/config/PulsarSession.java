package com.echostreams.pulsar.jms.config;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.common.AbstractDestination;
import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.AbstractSession;
import com.echostreams.pulsar.jms.common.destination.TemporaryQueueRef;
import com.echostreams.pulsar.jms.common.destination.TemporaryTopicRef;
import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;
import com.echostreams.pulsar.jms.queue.PulsarQueue;
import com.echostreams.pulsar.jms.queue.PulsarQueueBrowser;
import com.echostreams.pulsar.jms.topic.PulsarDurableTopicSubscriber;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.StringRelatedUtils;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.UUIDProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.util.List;
import java.util.Vector;

public class PulsarSession extends AbstractSession {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSession.class);

    // Attributes
    protected PulsarJMSProvider pulsarJMSProvider;

    // Runtime
    private List<AbstractMessage> pendingPuts = new Vector<>();
    //private TransactionSet transactionSet = new TransactionSet();
    //private boolean debugEnabled = log.isDebugEnabled();

    // For internal use by the remote layer
    //protected NotificationProxy notificationProxy;

    // Message stats
    private long consumedCount;
    private long producedCount;

    public PulsarSession(IntegerID sessionId, PulsarConnection pulsarConnection, PulsarJMSProvider pulsarJMSProvider, boolean transacted, int acknowledgeMode) {
        super(sessionId, pulsarConnection, transacted, acknowledgeMode);
        this.pulsarJMSProvider = pulsarJMSProvider;

    }

    @Override
    public void commit() throws JMSException {
        commit(true, null);
    }

    @Override
    public void rollback() throws JMSException {
        rollback(true, null);
    }

    @Override
    public void recover() throws JMSException {
        recover(null);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) throws JMSException
    {
        return createConsumer(idProvider.createID(), destination, messageSelector, noLocal);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscriptionName, String messageSelector, boolean noLocal) throws JMSException
    {
        return createDurableSubscriber(idProvider.createID(), topic, subscriptionName, messageSelector, noLocal);
    }

    @Override
    public QueueBrowser createBrowser(Queue queueRef, String messageSelector) throws JMSException {
        return createBrowser(idProvider.createID(), queueRef, messageSelector);
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            String queueName = "TEMP-QUEUE-"+ UUIDProvider.getInstance().getShortUUID();
            pulsarJMSProvider.createTemporaryQueue(queueName);
            connection.registerTemporaryQueue(queueName);

            return new TemporaryQueueRef(connection,queueName);
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            String topicName = "TEMP-TOPIC-"+UUIDProvider.getInstance().getShortUUID();
            pulsarJMSProvider.createTemporaryTopic(topicName);
            connection.registerTemporaryTopic(topicName);

            return new TemporaryTopicRef(connection,topicName);
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public void unsubscribe(String subscriptionName) throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            if (StringRelatedUtils.isEmpty(subscriptionName))
                throw new PulsarJMSException("Empty subscription name","INVALID_SUBSCRIPTION_NAME");

            // Remove remaining subscriptions on all topics
            pulsarJMSProvider.unsubscribe(connection.getClientID(), subscriptionName);
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public void acknowledge() throws JMSException {
        acknowledge(null);
    }

    public final void commit(boolean commitGets, List<String> deliveredMessageIDs) throws JMSException {
        if (!transacted)
            throw new javax.jms.IllegalStateException("Session is not transacted"); // [JMS SPEC]

        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            commitUpdates(commitGets, deliveredMessageIDs, true);
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    public final void acknowledge(List<String> deliveredMessageIDs) throws JMSException {
        if (transacted)
            throw new IllegalStateException("Session is transacted"); // [JMS SPEC]

        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            commitUpdates(true, deliveredMessageIDs, false);
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    private void commitUpdates(boolean commitGets, List<String> deliveredMessageIDs, boolean commitPuts) throws JMSException {
        /*SynchronizationBarrier commitBarrier = null;
        List<PulsarQueue> queuesWithGet = null;
        MessageLockSet locks = null;
        JMSException putFailure = null;
        Set<Committable> committables = new HashSet<>();

        // 1 - Build a list of queues updated in get operations
        if (commitGets && transactionSet.size() > 0)
        {
            if (deliveredMessageIDs != null)
                queuesWithGet = transactionSet.updatedQueues(deliveredMessageIDs);
            else
                queuesWithGet = transactionSet.updatedQueues();
        }

        // 2 - Build a list of all target destinations
        List<Committable> targetDestinations = computeLocalTargetDestinations(commitPuts ? pendingPuts : null,queuesWithGet);

        // 3 - Lock target destinations
        for (int i = 0; i < targetDestinations.size(); i++)
        {
            Committable committable = targetDestinations.get(i);
            committable.openTransaction();
        }
        try
        {
            if (commitPuts)
            {
                // 4 - Try sending all pending queue messages first (because this may fail if a queue is full)
                synchronized (pendingPuts)
                {
                    if (!pendingPuts.isEmpty())
                    {
                        int pendingSize = pendingPuts.size();
                        locks = new MessageLockSet(pendingSize);

                        if (debugEnabled)
                            log.debug(this+" - COMMIT [PUT] "+pendingPuts.size()+" message(s)");

                        // Put messages in locked state. They will be unlocked after proper commit.
                        try
                        {
                            for (int i = 0; i < pendingPuts.size(); i++)
                            {
                                AbstractMessage message = pendingPuts.get(i);
                                AbstractLocalDestination targetDestination = getLocalDestination(message);
                                if (targetDestination.putLocked(message, this, locks))
                                    committables.add(targetDestination);
                            }

                            // All messages successfully pushed
                            pendingPuts.clear();
                        }
                        catch (JMSException e)
                        {
                            if (transacted)
                            {
                                // Oops, something went wrong, we need to rollback what we have done yet
                                for (int i = 0; i < locks.size(); i++)
                                {
                                    MessageLock item = locks.get(i);
                                    item.getDestination().removeLocked(item);
                                }

                                // Store failure (will be re-thrown later after transaction commit, see below)
                                putFailure = e;
                            }
                            else
                            {
                                pendingPuts.clear(); // Make sure we discard the messages on failure, otherwise they will pile-up, which is unexpected in non-transacted mode
                                ErrorTools.log(e, log);
                            }
                        }

                        producedCount += pendingSize;
                    }
                }
            }

            // 5 - Commit pending get messages, i.e. delete them from destinations
            if (queuesWithGet != null && putFailure == null)
            {
                TransactionItem[] pendingGets;
                if (deliveredMessageIDs != null)
                {
                    // Commit only delivered messages
                    if (debugEnabled)
                        log.debug(this+" - COMMIT [GET] "+deliveredMessageIDs.size()+" message(s)");
                    pendingGets = transactionSet.clear(deliveredMessageIDs);
                }
                else
                {
                    // Commit the whole transaction set
                    if (debugEnabled)
                        log.debug(this+" - COMMIT [GET] "+transactionSet.size()+" message(s)");
                    pendingGets = transactionSet.clear();
                }

                for (int i = 0; i < queuesWithGet.size(); i++)
                {
                    LocalQueue localQueue = queuesWithGet.get(i);
                    if (localQueue.remove(this,pendingGets))
                        committables.add(localQueue);
                    consumedCount++;
                }
            }

            // 6 - Commit destinations
            if (committables.size() > 0)
            {
                commitBarrier = new SynchronizationBarrier();

                Iterator<Committable> commitables = committables.iterator();
                while (commitables.hasNext())
                {
                    Committable commitable = commitables.next();
                    commitable.commitChanges(commitBarrier);
                }
            }
        }
        finally
        {
            // 7 - Release locks
            for (int i = 0; i < targetDestinations.size(); i++)
            {
                Committable committable = targetDestinations.get(i);
                committable.closeTransaction();
            }
        }

        // 8 - If something went wrong during put operations, stop here
        if (putFailure != null)
            throw putFailure;

        // 9 - Wait for commit barrier if necessary
        if (commitBarrier != null)
        {
            try
            {
                commitBarrier.waitFor();
            }
            catch (InterruptedException e)
            {
                throw new JMSException("Commit barrier was interrupted");
            }
        }

        // 10 - Unlock and deliver messages
        if (locks != null)
        {
            for (int i = 0; i < locks.size(); i++)
            {
                MessageLock item = locks.get(i);
                item.getDestination().unlockAndDeliver(item);
            }
        }*/
    }

    public final void rollback(boolean rollbackGets, List<String> deliveredMessageIDs) throws JMSException {
        if (!transacted)
            throw new IllegalStateException("Session is not transacted"); // [JMS SPEC]

        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            rollbackUpdates(true, rollbackGets, deliveredMessageIDs);
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    private void rollbackUpdates(boolean rollbackPuts, boolean rollbackGets, List<String> deliveredMessageIDs) throws JMSException {
        // Clear pending put messages
        /*if (rollbackPuts && transacted)
        {
            if (!pendingPuts.isEmpty())
            {
                if (debugEnabled)
                    log.debug(this+" - ROLLBACK [PUT] "+pendingPuts.size()+" message(s)");

                pendingPuts.clear();
            }
        }

        // Rollback pending get messages
        if (rollbackGets && transactionSet.size() > 0)
        {
            SynchronizationBarrier commitBarrier = null;
            Set<Committable> committables = new HashSet<>();

            // 1 - Check for pending get operations
            TransactionItem[] pendingGets;
            if (deliveredMessageIDs != null)
            {
                // Rollback only delivered messages
                if (debugEnabled)
                    log.debug(this+" - ROLLBACK [GET] "+deliveredMessageIDs.size()+" message(s)");
                pendingGets = transactionSet.clear(deliveredMessageIDs);
            }
            else
            {
                // Rollback the whole transaction set
                if (debugEnabled)
                    log.debug(this+" - ROLLBACK [GET] "+transactionSet.size()+" message(s)");
                pendingGets = transactionSet.clear();
            }
            List<LocalQueue> queuesWithGet = computeUpdatedQueues(pendingGets);
            MessageLockSet locks = new MessageLockSet(pendingGets.length);

            // 2 - Compute target destinations lists
            List<Committable> targetDestinations = computeLocalTargetDestinations(null,queuesWithGet);

            // 3 - Lock target destinations
            for (int i = 0; i < targetDestinations.size(); i++)
            {
                Committable committable = targetDestinations.get(i);
                committable.openTransaction();
            }
            try
            {
                // 4 - Redeliver locked messages to queues
                for (int i = 0; i < queuesWithGet.size(); i++)
                {
                    LocalQueue localQueue = queuesWithGet.get(i);
                    if (localQueue.redeliverLocked(pendingGets,locks))
                        committables.add(localQueue);
                }

                // 5 - Commit destinations
                if (committables.size() > 0)
                {
                    commitBarrier = new SynchronizationBarrier();

                    Iterator<Committable> commitables = committables.iterator();
                    while (commitables.hasNext())
                    {
                        Committable commitable = commitables.next();
                        commitable.commitChanges(commitBarrier);
                    }
                }
            }
            finally
            {
                // 6 - Release locks
                for (int i = 0; i < targetDestinations.size(); i++)
                {
                    Committable committable = targetDestinations.get(i);
                    committable.closeTransaction();
                }
            }

            // 7 - Wait for commit barrier if necessary
            if (commitBarrier != null)
            {
                try
                {
                    commitBarrier.waitFor();
                }
                catch (InterruptedException e)
                {
                    throw new JMSException("Commit barrier was interrupted");
                }
            }

            // 8 - Unlock and re-deliver messages if necessary
            for (int i = 0; i < locks.size(); i++)
            {
                MessageLock item = locks.get(i);
                item.getDestination().unlockAndDeliver(item);
            }
        }*/
    }

    public final void recover(List<String> deliveredMessageIDs) throws JMSException {
        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            if (transacted)
                throw new IllegalStateException("Session is transacted"); // [JMS SPEC]

            rollbackUpdates(true, true, deliveredMessageIDs);
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    public MessageConsumer createConsumer(IntegerID consumerId,Destination destination, String messageSelector, boolean noLocal) throws JMSException
    {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            PulsarMessageConsumer consumer = new PulsarMessageConsumer(pulsarJMSProvider,this,destination,messageSelector,noLocal,consumerId,null);
            registerConsumer(consumer);
            consumer.initDestination();
            return consumer;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    private AbstractDestination getLocalDestination( AbstractMessage message ) throws JMSException
    {
        Destination destination = message.getJMSDestination();

        if (destination instanceof Queue)
        {
            Queue queueRef = (Queue)destination;
            return pulsarJMSProvider.getPulsarQueue(queueRef.getQueueName());
        }
        else
        if (destination instanceof Topic)
        {
            Topic topicRef = (Topic)destination;
            return pulsarJMSProvider.getPulsarTopic(topicRef.getTopicName());
        }
        else
            throw new InvalidDestinationException("Unsupported destination : "+destination);
    }

    public QueueBrowser createBrowser(IntegerID browserId,Queue queueRef, String messageSelector) throws JMSException
    {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            PulsarQueue pulsarQueue = pulsarJMSProvider.getPulsarQueue(queueRef.getQueueName());

            // Check temporary destinations scope (JMS Spec 4.4.3 p2)
            checkTemporaryDestinationScope(pulsarQueue);

           PulsarQueueBrowser browser = new PulsarQueueBrowser(this,pulsarQueue,messageSelector,browserId);
            registerBrowser(browser);
            return browser;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    public TopicSubscriber createDurableSubscriber(IntegerID consumerId, Topic topic, String subscriptionName, String messageSelector, boolean noLocal) throws JMSException
    {
        if (StringRelatedUtils.isEmpty(subscriptionName))
            throw new PulsarJMSException("Empty subscription name","INVALID_SUBSCRIPTION_NAME");

        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();

            // Get the client ID
            String clientID = connection.getClientID();

            // Create the consumer
            String subscriberId = clientID+"-"+subscriptionName;
            PulsarDurableTopicSubscriber subscriber = new PulsarDurableTopicSubscriber(pulsarJMSProvider,this,topic,messageSelector,noLocal,consumerId,subscriberId);
            registerConsumer(subscriber);
            subscriber.initDestination();

            // Register the subscription
            pulsarJMSProvider.subscribe(clientID, subscriptionName);

            return subscriber;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }


}
