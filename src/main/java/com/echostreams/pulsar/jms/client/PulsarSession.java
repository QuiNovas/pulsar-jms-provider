package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.message.*;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.Properties;

public class PulsarSession implements Session, QueueSession, TopicSession {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarSession.class);

    private Properties config;
    private PulsarMessageProducer producer;
    private PulsarMessageConsumer consumer;
    private MessageListener listener;
    private PulsarConnection connection;
    private int acknowledgeMode;
    private boolean transacted;

    public PulsarSession(PulsarConnection connection, boolean transacted, int acknowledgeMode) {
        this.connection = connection;
        this.transacted = transacted;
        this.acknowledgeMode = acknowledgeMode;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createBytesMessage()
     */
    @Override
    public BytesMessage createBytesMessage() throws JMSException {
        return new PulsarBytesMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createMapMessage()
     */
    @Override
    public MapMessage createMapMessage() throws JMSException {
        return new PulsarMapMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createMessage()
     */
    @Override
    public Message createMessage() throws JMSException {
        return createObjectMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createObjectMessage()
     */
    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return new PulsarObjectMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createObjectMessage(java.io.Serializable)
     */
    @Override
    public ObjectMessage createObjectMessage(Serializable object)
            throws JMSException {
        PulsarObjectMessage msg = new PulsarObjectMessage();
        msg.setObject(object);
        return msg;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createStreamMessage()
     */
    @Override
    public StreamMessage createStreamMessage() throws JMSException {
        return new PulsarStreamMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTextMessage()
     */
    @Override
    public TextMessage createTextMessage() throws JMSException {
        return new PulsarTextMessage();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTextMessage(java.lang.String)
     */
    @Override
    public TextMessage createTextMessage(String text) throws JMSException {
        PulsarTextMessage msg = new PulsarTextMessage();
        msg.setText(text);
        return msg;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#getTransacted()
     */
    @Override
    public boolean getTransacted() throws JMSException {
        return this.transacted;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#getAcknowledgeMode()
     */
    @Override
    public int getAcknowledgeMode() throws JMSException {
        return this.acknowledgeMode;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#commit()
     */
    @Override
    public void commit() throws JMSException {
        consumer.commit();
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#rollback()
     */
    @Override
    public void rollback() throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Session#close()
     */
    @Override
    public void close() throws JMSException {
        try {
            producer.close();
            consumer.close();
        } catch (Exception e) {
        }
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#recover()
     */
    @Override
    public void recover() throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Session#getMessageListener()
     */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#setMessageListener(javax.jms.MessageListener)
     */
    @Override
    public void setMessageListener(MessageListener listener)
            throws JMSException {
        this.listener = listener;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#run()
     */
    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createProducer(javax.jms.Destination)
     */
    @Override
    public PulsarMessageProducer createProducer(Destination destination) throws JMSException {

        if (destination == null)
            throw new java.lang.IllegalArgumentException("destination may not be null!");

        try {
            producer = new PulsarMessageProducer(destination, this);
        } catch (PulsarClientException e) {
            LOGGER.error("Pulsar createProducer exception", e);
        }
        return producer;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public PulsarMessageConsumer createConsumer(Destination destination)
            throws JMSException {
        return createConsumer(destination, null, false);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public PulsarMessageConsumer createConsumer(Destination destination,
                                                String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String, boolean)
     */
    @Override
    public PulsarMessageConsumer createConsumer(Destination destination,
                                                String messageSelector, boolean noLocal) throws JMSException {
        if (destination == null)
            throw new java.lang.IllegalArgumentException("destination may not be null!");

        consumer = new PulsarMessageConsumer(destination, messageSelector, this);
        return consumer;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic,
                                                String sharedSubscriptionName) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createSharedConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedConsumer(Topic topic,
                                                String sharedSubscriptionName, String messageSelector)
            throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createQueue(java.lang.String)
     */
    @Override
    public Queue createQueue(String queueName) throws JMSException {
        return new PulsarQueue(queueName);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return createReceiver(queue, null);
    }

    @Override
    public QueueReceiver createReceiver(Queue queue, String messageSelector) throws JMSException {
        return createConsumer(queue, messageSelector);
    }

    @Override
    public QueueSender createSender(Queue queue) throws JMSException {
        return createProducer(queue);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTopic(java.lang.String)
     */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return new PulsarTopic(topicName);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return createSubscriber(topic, null, false);
    }

    @Override
    public TopicSubscriber createSubscriber(Topic topic, String messageSelector, boolean noLocal) throws JMSException {
        return createConsumer(topic, messageSelector, noLocal);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name,
                                                   String messageSelector, boolean noLocal) throws JMSException {
        if (topic == null)
            throw new java.lang.IllegalArgumentException("topic is null!");
        if (name == null)
            throw new java.lang.IllegalArgumentException("name is null!");

        PulsarMessageConsumer pulsarMessageConsumer =
                new PulsarMessageConsumer((PulsarDestination) topic, messageSelector, this);

        pulsarMessageConsumer.setNoLocal(noLocal);
        pulsarMessageConsumer.setDurable(true);
        return pulsarMessageConsumer;
    }

    @Override
    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return createProducer(topic);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name)
            throws JMSException {
        return createDurableConsumer(topic, name, null, false);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableConsumer(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name,
                                                 String messageSelector, boolean noLocal) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name)
            throws JMSException {
        return createSharedDurableConsumer(topic, name, null);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createSharedDurableConsumer(javax.jms.Topic, java.lang.String, java.lang.String)
     */
    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic,
                                                       String name, String messageSelector) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createBrowser(javax.jms.Queue)
     */
    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new UnsupportedOperationException("Browsing is not supported for Pulsar");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException {
        throw new UnsupportedOperationException("Browsing is not supported for Pulsar");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTemporaryQueue()
     */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new UnsupportedOperationException("Temporary queues are not supported for Pulsar");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTemporaryTopic()
     */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new UnsupportedOperationException("Temporary topics are not supported for Pulsar");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String name) throws JMSException {
        consumer.unsubscribe();
    }


    public PulsarConnection getConnection() {
        return connection;
    }
}
