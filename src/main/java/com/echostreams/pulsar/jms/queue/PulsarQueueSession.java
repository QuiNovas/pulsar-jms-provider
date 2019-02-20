package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;
import javax.jms.IllegalStateException;

public class PulsarQueueSession extends PulsarSession implements QueueSession {

    public PulsarQueueSession(IntegerID id, PulsarQueueConnection connection, PulsarJMSProvider pulsarJMSProvider, boolean transacted, int acknowledgeMode) {
        super(id, connection, pulsarJMSProvider, transacted, acknowledgeMode);

    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        externalAccessLock.readLock().lock();
        try {
            checkNotClosed();
            PulsarQueueReceiver receiver = new PulsarQueueReceiver(pulsarJMSProvider, this, queue, messageSelector, idProvider.createID());
            registerConsumer(receiver);
            receiver.initDestination();
            return receiver;
        } finally {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            PulsarQueueSender sender = new PulsarQueueSender(this,queue,idProvider.createID());
            registerProducer(sender);
            return sender;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String subscriptionName, String messageSelector, boolean noLocal) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public void unsubscribe(String subscriptionName) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }
}
