package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;
import javax.jms.IllegalStateException;

public class PulsarTopicSession extends PulsarSession implements TopicSession {

    public PulsarTopicSession(IntegerID id, PulsarTopicConnection connection, PulsarJMSProvider pulsarJMSProvider, boolean transacted, int acknowledgeMode) {
        super(id, connection, pulsarJMSProvider, transacted, acknowledgeMode);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic,null,false);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException
    {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            PulsarTopicSubscriber subscriber = new PulsarTopicSubscriber(pulsarJMSProvider,this,topic,messageSelector,noLocal,idProvider.createID(),null);
            registerConsumer(subscriber);
            subscriber.initDestination();

            return subscriber;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        externalAccessLock.readLock().lock();
        try
        {
            checkNotClosed();
            PulsarTopicPublisher publisher = new PulsarTopicPublisher(this,topic,idProvider.createID());
            registerProducer(publisher);
            return publisher;
        }
        finally
        {
            externalAccessLock.readLock().unlock();
        }
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public synchronized QueueBrowser createBrowser(Queue queueRef, String messageSelector) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException
    {
        throw new IllegalStateException("Method not available on this domain.");
    }
}
