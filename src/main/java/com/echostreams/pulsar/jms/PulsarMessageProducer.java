package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarConnection;
import com.echostreams.pulsar.jms.client.PulsarDestination;
import com.echostreams.pulsar.jms.utils.MessageUtils;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Properties;

public class PulsarMessageProducer implements MessageProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageProducer.class);
    private static final int DEFAULT_PRIORITY = 4;
    private static final int DEFAULT_DELIERY_MODE = DeliveryMode.PERSISTENT;
    private static final int DEFAULT_TTL = 60000;
    private Producer producer;
    private PulsarDestination destination;
    private boolean disbledMessageId;
    private boolean disableMessageTimestamp;
    private int deliveryMode = DEFAULT_DELIERY_MODE;
    private int priority = DEFAULT_PRIORITY;
    private long timeToLive = DEFAULT_TTL;
    private long deliveryDelay;

    /**
     * @param config
     * @param destination
     * @param connection
     */
    public PulsarMessageProducer(Properties config, Destination destination, PulsarConnection connection) throws PulsarClientException, JMSException {
        this.destination = (PulsarDestination) destination;
        //this.producer = new KafkaProducer<String, Message>(config);
        //TODO need to map with pulsar producer


        this.producer = new ProducerBuilderImpl((PulsarClientImpl) connection.getClient(), Schema.STRING).topic(((PulsarDestination) destination).getName()).create();
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setDisableMessageID(boolean)
     */
    @Override
    public void setDisableMessageID(boolean value) throws JMSException {
        this.disbledMessageId = value;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getDisableMessageID()
     */
    @Override
    public boolean getDisableMessageID() throws JMSException {
        return this.disbledMessageId;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setDisableMessageTimestamp(boolean)
     */
    @Override
    public void setDisableMessageTimestamp(boolean value) throws JMSException {
        this.disableMessageTimestamp = value;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getDisableMessageTimestamp()
     */
    @Override
    public boolean getDisableMessageTimestamp() throws JMSException {
        return this.disableMessageTimestamp;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setDeliveryMode(int)
     */
    @Override
    public void setDeliveryMode(int deliveryMode) throws JMSException {
        this.deliveryMode = deliveryMode;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getDeliveryMode()
     */
    @Override
    public int getDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setPriority(int)
     */
    @Override
    public void setPriority(int defaultPriority) throws JMSException {
        this.priority = defaultPriority;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getPriority()
     */
    @Override
    public int getPriority() throws JMSException {
        // TODO Auto-generated method stub
        return priority;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setTimeToLive(long)
     */
    @Override
    public void setTimeToLive(long timeToLive) throws JMSException {
        this.timeToLive = timeToLive;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getTimeToLive()
     */
    @Override
    public long getTimeToLive() throws JMSException {
        // producer.metrics().get(ProducerConfig.METADATA_MAX_AGE_CONFIG);
        return this.timeToLive;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#setDeliveryDelay(long)
     */
    @Override
    public void setDeliveryDelay(long deliveryDelay) throws JMSException {
        this.deliveryDelay = deliveryDelay;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getDeliveryDelay()
     */
    @Override
    public long getDeliveryDelay() throws JMSException {
        return this.deliveryDelay;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#getDestination()
     */
    @Override
    public Destination getDestination() throws JMSException {
        return destination;
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#close()
     */
    @Override
    public void close() {
        try {
            producer.close();
        } catch (PulsarClientException e) {

        }
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Message)
     */
    @Override
    public void send(Message message) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Message message, int deliveryMode, int priority,
                     long timeToLive) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     * javax.jms.Message)
     */
    @Override
    public void send(Destination destination, Message message)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     * javax.jms.Message, int, int, long)
     */
    @Override
    public void send(Destination destination, Message message,
                     int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        // Send each message and log message content and ID when successfully received
        MessageId msgId = null;
        try {
            Message transformedMessage = MessageUtils.transformMessage(message);
            if (transformedMessage != null) {
                message = transformedMessage;
            }
            msgId = producer.send(message);
        } catch (PulsarClientException e) {
            LOGGER.error("PulsarClientException during send : ", e);
        }
        LOGGER.info("Published msg='{}' with msg-id={}", message, msgId);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Message,
     * javax.jms.CompletionListener)
     */
    @Override
    public void send(Message message, CompletionListener completionListener)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);

    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Message, int, int, long,
     * javax.jms.CompletionListener)
     */
    @Override
    public void send(Message message, int deliveryMode, int priority,
                     long timeToLive, CompletionListener completionListener)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     * javax.jms.Message, javax.jms.CompletionListener)
     */
    @Override
    public void send(Destination destination, Message message,
                     CompletionListener completionListener) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);
    }

    /*
     * (non-Javadoc)
     *
     * @see javax.jms.MessageProducer#send(javax.jms.Destination,
     * javax.jms.Message, int, int, long, javax.jms.CompletionListener)
     */
    @Override
    public void send(Destination destination, Message message,
                     int deliveryMode, int priority, long timeToLive,
                     CompletionListener completionListener) throws JMSException {

        if (completionListener == null) {
            throw new IllegalArgumentException("CompletetionListener cannot be null");
        }
        //TODO need to map with pulsar producer send
        // Send each message and log message content and ID when successfully received
        MessageId msgId = null;
        try {
            msgId = producer.send(message);
        } catch (PulsarClientException e) {
            LOGGER.error(e.getMessage());
        }
        LOGGER.info("Published msg='{}' with msg-id={}", message, msgId);
    }

}
