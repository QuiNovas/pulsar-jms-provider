package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.PulsarMessageConsumer;
import com.echostreams.pulsar.jms.PulsarMessageProducer;
import com.echostreams.pulsar.jms.PulsarQueue;
import com.echostreams.pulsar.jms.PulsarTopic;
import com.echostreams.pulsar.jms.message.*;

import javax.jms.*;
import java.io.Serializable;
import java.util.Properties;

public class PulsarSession implements Session {
    private Properties config;
    private PulsarMessageProducer producer;
    private PulsarMessageConsumer consumer;

    private MessageListener listener;

    PulsarConnection connection;

    public PulsarSession(PulsarConnection connection) {
        this.connection = connection;
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
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#getAcknowledgeMode()
     */
    @Override
    public int getAcknowledgeMode() throws JMSException {
        return 0;
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
    public MessageProducer createProducer(Destination destination)
            throws JMSException {
        producer = new PulsarMessageProducer(config, destination, connection);
        return producer;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination)
            throws JMSException {
        consumer = new PulsarMessageConsumer(config, destination);
        return consumer;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination,
                                          String messageSelector) throws JMSException {
        return createConsumer(destination);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createConsumer(javax.jms.Destination, java.lang.String, boolean)
     */
    @Override
    public MessageConsumer createConsumer(Destination destination,
                                          String messageSelector, boolean noLocal) throws JMSException {
        return createConsumer(destination);
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

    /* (non-Javadoc)
     * @see javax.jms.Session#createTopic(java.lang.String)
     */
    @Override
    public Topic createTopic(String topicName) throws JMSException {
        return new PulsarTopic(topicName);
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name)
            throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableSubscriber(javax.jms.Topic, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name,
                                                   String messageSelector, boolean noLocal) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createDurableConsumer(javax.jms.Topic, java.lang.String)
     */
    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name)
            throws JMSException {
        // TODO Auto-generated method stub
        return null;
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
        // TODO Auto-generated method stub
        return null;
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
        throw new UnsupportedOperationException("Browsing is not supported for Kafka");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createBrowser(javax.jms.Queue, java.lang.String)
     */
    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException {
        throw new UnsupportedOperationException("Browsing is not supported for Kafka");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTemporaryQueue()
     */
    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new UnsupportedOperationException("Temporary queues are not supported for Kafka");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#createTemporaryTopic()
     */
    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new UnsupportedOperationException("Temporary topics are not supported for Kafka");
    }

    /* (non-Javadoc)
     * @see javax.jms.Session#unsubscribe(java.lang.String)
     */
    @Override
    public void unsubscribe(String name) throws JMSException {
        consumer.unsubscribe();
    }

}
