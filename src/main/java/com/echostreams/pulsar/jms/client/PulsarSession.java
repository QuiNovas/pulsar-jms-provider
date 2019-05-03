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
        return createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() throws JMSException {
        return new PulsarObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable object)
            throws JMSException {
        PulsarObjectMessage msg = new PulsarObjectMessage();
        msg.setObject(object);
        return msg;
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
        PulsarTextMessage msg = new PulsarTextMessage();
        msg.setText(text);
        return msg;
    }

    @Override
    public boolean getTransacted() throws JMSException {
        return this.transacted;
    }

    @Override
    public int getAcknowledgeMode() throws JMSException {
        return this.acknowledgeMode;
    }

    @Override
    public void commit() throws JMSException {
        consumer.commit();
    }

    @Override
    public void rollback() throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void close() throws JMSException {
        try {
            producer.close();
            consumer.close();
        } catch (Exception e) {
        }
    }

    @Override
    public void recover() throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws JMSException {
        this.listener = listener;
    }

    @Override
    public void run() {
        // TODO Auto-generated method stub

    }

    @Override
    public PulsarMessageProducer createProducer(Destination destination) throws JMSException {

        if (destination == null)
            throw new java.lang.IllegalArgumentException("destination may not be null!");

        try {
            producer = new PulsarMessageProducer(destination, this);
        } catch (PulsarClientException e) {
            LOGGER.error("Error while creating Producer", e);
            System.exit(-1);
        }
        return producer;
    }

    @Override
    public PulsarMessageConsumer createConsumer(Destination destination)
            throws JMSException {
        return createConsumer(destination, null, false);
    }

    @Override
    public PulsarMessageConsumer createConsumer(Destination destination,
                                                String messageSelector) throws JMSException {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public PulsarMessageConsumer createConsumer(Destination destination,
                                                String messageSelector, boolean noLocal) throws JMSException {
        if (destination == null)
            throw new java.lang.IllegalArgumentException("destination may not be null!");

        consumer = new PulsarMessageConsumer(destination, messageSelector, this);
        return consumer;
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic,
                                                String sharedSubscriptionName) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageConsumer createSharedConsumer(Topic topic,
                                                String sharedSubscriptionName, String messageSelector)
            throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

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

    @Override
    public TopicSubscriber createDurableSubscriber(Topic topic, String name) throws JMSException {
        return createDurableSubscriber(topic, name, null, false);
    }

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

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name)
            throws JMSException {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public MessageConsumer createDurableConsumer(Topic topic, String name,
                                                 String messageSelector, boolean noLocal) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic, String name)
            throws JMSException {
        return createSharedDurableConsumer(topic, name, null);
    }

    @Override
    public MessageConsumer createSharedDurableConsumer(Topic topic,
                                                       String name, String messageSelector) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        throw new UnsupportedOperationException("Browsing is not supported for Pulsar");
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String messageSelector)
            throws JMSException {
        throw new UnsupportedOperationException("Browsing is not supported for Pulsar");
    }

    @Override
    public TemporaryQueue createTemporaryQueue() throws JMSException {
        throw new UnsupportedOperationException("Temporary queues are not supported for Pulsar");
    }

    @Override
    public TemporaryTopic createTemporaryTopic() throws JMSException {
        throw new UnsupportedOperationException("Temporary topics are not supported for Pulsar");
    }

    @Override
    public void unsubscribe(String name) throws JMSException {
        consumer.unsubscribe();
    }

    public PulsarConnection getConnection() {
        return connection;
    }
}
