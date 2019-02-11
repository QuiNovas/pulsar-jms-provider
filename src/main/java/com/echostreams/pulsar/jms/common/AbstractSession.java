package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.common.destination.QueueRef;
import com.echostreams.pulsar.jms.common.destination.TopicRef;
import com.echostreams.pulsar.jms.message.*;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.id.IntegerID;
import com.echostreams.pulsar.jms.utils.id.IntegerIDProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractSession implements Session {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSession.class);

    // Parent connection
    protected AbstractConnection connection;

    // Attributes
    protected IntegerID id;
    protected boolean transacted;
    protected int acknowledgeMode;
    protected boolean closed;
    public Object deliveryLock = new Object();

    // Children
    protected Map<IntegerID,AbstractMessageConsumer> consumersMap = new Hashtable<>();
    private Map<IntegerID,AbstractMessageProducer> producersMap = new Hashtable<>();
    private Map<IntegerID,AbstractQueueBrowser> browsersMap = new Hashtable<>();

    // Runtime
    protected IntegerIDProvider idProvider = new IntegerIDProvider();
    protected ReadWriteLock externalAccessLock = new ReentrantReadWriteLock();

    public AbstractSession( AbstractConnection connection , boolean transacted , int acknowledgeMode )
    {
        this.connection = connection;
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
    }

    public AbstractSession( IntegerID id , AbstractConnection connection , boolean transacted , int acknowledgeMode )
    {
        this(connection,transacted,acknowledgeMode);
        this.id = id;
    }


    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return new PulsarBytesMessage();
    }

    @Override
    public MapMessage createMapMessage() throws JMSException {
        return new PulsarMapMessage();
    }

    @Override
    public Message createMessage() throws JMSException {
        return new PulsarEmptyMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return new PulsarObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) throws JMSException {
        return new PulsarObjectMessage(serializable);
    }

    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return new PulsarStreamMessage();
    }

    @Override
    public TextMessage createTextMessage() throws JMSException {
        return new PulsarTextMessage();
    }

    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        return new PulsarTextMessage(text);
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return transacted;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        if (transacted)
            return Session.SESSION_TRANSACTED; // [JMS Spec]
        return acknowledgeMode;
    }

    @Override
    public void close() throws JMSException {
        externalAccessLock.writeLock().lock();
        try
        {
            if (closed)
                return;
            closed = true;
            onSessionClose();
        }
        finally
        {
            externalAccessLock.writeLock().unlock();
        }
        onSessionClosed();
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSException {
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }

    @Override
    public void run() {

    }

    @Override
    public MessageProducer createProducer(Destination destination) throws JMSException {
        return null;
    }

    @Override
    public MessageConsumer createConsumer(Destination destination) throws JMSException {
        return createConsumer(destination,null,false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector) throws JMSException {
        return createConsumer(destination,messageSelector,false);
    }

    @Override
    public MessageConsumer createConsumer(Destination destination, String messageSelector, boolean b) throws JMSException {
        return createConsumer(destination,messageSelector,false);
    }

    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return new QueueRef(queueName);
    }

    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return new TopicRef(topicName);
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String s) throws JMSException {
        return null;
    }

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String s, String s1, boolean b) throws JMSException {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String s) throws JMSException {
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return null;
    }

    @Override
    public void unsubscribe(String s) throws JMSException {

    }

    protected void onSessionClose()
    {
        connection.unregisterSession(this);
        closeRemainingConsumers();
        closeRemainingProducers();
        closeRemainingBrowsers();
    }

    protected void onSessionClosed()
    {
        // Nothing
    }

    private void closeRemainingConsumers()
    {
        List<AbstractMessageConsumer> consumersToClose = new ArrayList<>(consumersMap.size());
        synchronized (consumersMap)
        {
            consumersToClose.addAll(consumersMap.values());
        }
        for (int n = 0 ; n < consumersToClose.size() ; n++)
        {
            MessageConsumer consumer = consumersToClose.get(n);
            LOGGER.debug("Auto-closing unclosed consumer : "+consumer);
            try
            {
                consumer.close();
            }
            catch (Exception e)
            {
                LOGGER.error("Could not close consumer "+consumer,e);
            }
        }
    }

    private void closeRemainingProducers()
    {
        List<AbstractMessageProducer> producersToClose = new ArrayList<>(producersMap.size());
        synchronized (producersMap)
        {
            producersToClose.addAll(producersMap.values());
        }
        for (int n = 0 ; n < producersToClose.size() ; n++)
        {
            MessageProducer producer = producersToClose.get(n);
            LOGGER.debug("Auto-closing unclosed producer : "+producer);
            try
            {
                producer.close();
            }
            catch (Exception e)
            {
                LOGGER.error("Could not close producer "+producer,e);
            }
        }
    }

    private void closeRemainingBrowsers()
    {
        List<AbstractQueueBrowser> browsersToClose = new ArrayList<>(browsersMap.size());
        synchronized (browsersMap)
        {
            browsersToClose.addAll(browsersMap.values());
        }
        for (int n = 0 ; n < browsersToClose.size() ; n++)
        {
            QueueBrowser browser = browsersToClose.get(n);
            LOGGER.debug("Auto-closing unclosed browser : "+browser);
            try
            {
                browser.close();
            }
            catch (Exception e)
            {
                LOGGER.error("Could not close browser "+browser,e);
            }
        }
    }

    protected final void checkNotClosed() throws JMSException
    {
        if (closed)
            throw new IllegalStateException("Session is closed"); // [JMS SPEC]
    }

    protected final void registerConsumer( AbstractMessageConsumer consumer )
    {
        if (consumersMap.put(consumer.getId(),consumer) != null)
            throw new IllegalArgumentException("Consumer "+consumer.getId()+" already exists");
    }

    public final AbstractConnection getConnection()
    {
        return connection;
    }

    protected final void unregisterConsumer( AbstractMessageConsumer consumerToRemove )
    {
        if (consumersMap.remove(consumerToRemove.getId()) == null)
            LOGGER.warn("Unknown consumer : "+consumerToRemove);
    }

    public abstract void acknowledge() throws JMSException;


}
