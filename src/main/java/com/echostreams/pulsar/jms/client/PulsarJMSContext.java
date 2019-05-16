package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.common.PulsarConnectionMetaDataImpl;
import com.echostreams.pulsar.jms.message.*;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Serializable;

public class PulsarJMSContext implements JMSContext {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSContext.class);

    private static ConnectionMetaData metaData = new PulsarConnectionMetaDataImpl();

    private PulsarJMSProducer producer;
    private PulsarJMSConsumer consumer;
    private MessageListener listener;
    private PulsarClient client;
    private Destination destination;
    private int sessionMode;

    public PulsarJMSContext(PulsarClient client, int sessionMode) throws JMSException {
        this.client = client;
        this.sessionMode = sessionMode;
    }

    @Override
    public JMSContext createContext(int sessionMode) {
        PulsarJMSContext pulsarJMSContext = null;
        try {
            pulsarJMSContext = new PulsarJMSContext(client, sessionMode);
        } catch (JMSException e) {
            LOGGER.error(" Exception during createContext: ", e);
        }
        return pulsarJMSContext;
    }

    @Override
    public JMSProducer createProducer() {
        try {
            producer = new PulsarJMSProducer(this);
        } catch (PulsarClientException e) {
            LOGGER.error("Pulsar createProducer PulsarClientException ", e);
        } catch (JMSException e) {
            LOGGER.error("Pulsar createProducer JMSException ", e);
        }
        return producer;
    }

    @Override
    public String getClientID() {
        return null;
    }

    @Override
    public void setClientID(String s) {

    }

    @Override
    public ConnectionMetaData getMetaData() {
        return metaData;
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return null;
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void setAutoStart(boolean b) {

    }

    @Override
    public boolean getAutoStart() {
        return false;
    }

    @Override
    public void close() {
        try {
            consumer.close();
            client.close();
        } catch (Exception e) {
        }
    }

    @Override
    public BytesMessage createBytesMessage() {
        PulsarBytesMessage pulsarBytesMessage = null;
        try {
            pulsarBytesMessage = new PulsarBytesMessage();
        } catch (JMSException e) {
            LOGGER.error("createBytesMessage error", e);
        }
        return pulsarBytesMessage;
    }

    @Override
    public MapMessage createMapMessage() {
        PulsarMapMessage pulsarMapMessage = null;
        try {
            pulsarMapMessage = new PulsarMapMessage();
        } catch (JMSException e) {
            LOGGER.error("createMapMessage error", e);
        }
        return pulsarMapMessage;
    }

    @Override
    public Message createMessage() {
        return createObjectMessage();
    }

    @Override
    public ObjectMessage createObjectMessage() {
        PulsarObjectMessage pulsarObjectMessage = null;
        try {
            pulsarObjectMessage = new PulsarObjectMessage();
        } catch (JMSException e) {
            LOGGER.error("createObjectMessage error", e);
        }
        return pulsarObjectMessage;
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) {
        PulsarObjectMessage pulsarObjectMessage = null;
        try {
            pulsarObjectMessage = new PulsarObjectMessage();
            pulsarObjectMessage.setObject(serializable);
        } catch (JMSException e) {
            LOGGER.error("createObjectMessage error", e);
        }
        return pulsarObjectMessage;
    }

    @Override
    public StreamMessage createStreamMessage() {
        PulsarStreamMessage pulsarStreamMessage = null;
        try {
            pulsarStreamMessage = new PulsarStreamMessage();
        } catch (JMSException e) {
            LOGGER.error("createStreamMessage error", e);
        }
        return pulsarStreamMessage;
    }

    @Override
    public TextMessage createTextMessage() {
        PulsarTextMessage pulsarTextMessage = null;
        try {
            pulsarTextMessage = new PulsarTextMessage();
        } catch (JMSException e) {
            LOGGER.error("createTextMessage error", e);
        }
        return pulsarTextMessage;
    }

    @Override
    public TextMessage createTextMessage(String value) {
        PulsarTextMessage pulsarTextMessage = null;
        try {
            pulsarTextMessage = new PulsarTextMessage();
            pulsarTextMessage.setText(value);
        } catch (JMSException e) {
            LOGGER.error("createTextMessage error", e);
        }
        return pulsarTextMessage;
    }

    @Override
    public boolean getTransacted() {
        return (JMSContext.SESSION_TRANSACTED == sessionMode);
    }

    @Override
    public int getSessionMode() {
        return sessionMode;
    }

    @Override
    public void commit() {
    }

    @Override
    public void rollback() {

    }

    @Override
    public void recover() {

    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return createConsumer(destination, null, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector) {
        return createConsumer(destination, messageSelector, false);
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String messageSelector, boolean noLocal) {
        try {
            if (destination == null)
                throw new java.lang.IllegalArgumentException("destination may not be null!");

            consumer = new PulsarJMSConsumer(destination, messageSelector, this);
        } catch (JMSException e) {
            LOGGER.error("createConsumer JMSException ", e);
        }
        return consumer;
    }

    @Override
    public Queue createQueue(String queueName) {
        PulsarQueue pulsarQueue = new PulsarQueue(queueName);
        this.destination = pulsarQueue;
        return pulsarQueue;
    }

    @Override
    public Topic createTopic(String topicName) {
        PulsarTopic pulsarTopic = new PulsarTopic(topicName);
        this.destination = pulsarTopic;
        return pulsarTopic;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String name) {
        return createDurableConsumer(topic, name, null, false);
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String s, String s1, boolean b) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String s) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String s, String s1) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String s) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String s, String s1) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String s) {
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return null;
    }

    @Override
    public void unsubscribe(String s) {
        try {
            consumer.unsubscribe();
        } catch (JMSException e) {
            LOGGER.error("JMSContext unsubscribe JMSException ", e);
        }
    }

    @Override
    public void acknowledge() {
    }

    public PulsarClient getClient() {
        return client;
    }

    public Destination getDestination() {
        return destination;
    }
}
