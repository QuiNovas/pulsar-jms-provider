package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.utils.DestinationUtils;
import com.echostreams.pulsar.jms.utils.MessageUtils;
import com.echostreams.pulsar.jms.utils.ObjectSerializer;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Date;

public class PulsarMessageProducer implements MessageProducer, QueueSender, TopicPublisher {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageProducer.class);
    private static final int DEFAULT_PRIORITY = 4;
    private static final int DEFAULT_DELIERY_MODE = DeliveryMode.PERSISTENT;
    private static final int DEFAULT_TTL = 60000;

    private Producer<byte[]> producer;
    private PulsarDestination destination;
    private boolean disbledMessageId;
    private boolean disableMessageTimestamp;
    private int deliveryMode = DEFAULT_DELIERY_MODE;
    private int priority = DEFAULT_PRIORITY;
    private long timeToLive = DEFAULT_TTL;
    private long deliveryDelay;
    private PulsarSession session;


    /**
     * @param destination
     * @param session
     */
    public PulsarMessageProducer(Destination destination, PulsarSession session) throws PulsarClientException, JMSException {
        this.destination = (PulsarDestination) destination;
        this.session = session;
        //TODO need to map with pulsar producer config
        this.producer = new ProducerBuilderImpl((PulsarClientImpl) session.getConnection().getClient(), Schema.BYTES).topic(((PulsarDestination) destination).getName()).create();
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

    @Override
    public Queue getQueue() throws JMSException {
        return null;
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

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        send((Destination) queue, message);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        send((Destination) queue, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void send(Destination destination, Message message,
                     int deliveryMode, int priority, long timeToLive)
            throws JMSException {
        // Send each message and log message content and ID when successfully received
        MessageId msgId = null;
        try {
            if (message == null) {
                throw new MessageFormatException("Null message");
            }

            message.setJMSReplyTo(DestinationUtils.transformDestination(destination));
            message.setJMSDestination(DestinationUtils.transformDestination(destination));
            message.setJMSTimestamp((new Date()).getTime());
            message.setJMSPriority(priority);

            if (timeToLive > 0) {
                message.setJMSExpiration(System.currentTimeMillis() + timeToLive);
            } else {
                message.setJMSExpiration(0);
            }

            Message transformedMessage = MessageUtils.transformMessage(message);
            if (transformedMessage != null) {
                message = transformedMessage;
            }
            msgId = producer.send(new ObjectSerializer().objectToByteArray(message));
            message.setJMSMessageID(msgId.toString());
        } catch (PulsarClientException e) {
            LOGGER.error("PulsarClientException during send : ", e);
        }
        LOGGER.info("Published msg='{}' with msg-id={}", message.getBody(message.getJMSType().getClass()), msgId);
    }

    @Override
    public void send(Message message, CompletionListener completionListener)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);

    }

    @Override
    public void send(Message message, int deliveryMode, int priority,
                     long timeToLive, CompletionListener completionListener)
            throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);
    }

    @Override
    public void send(Destination destination, Message message,
                     CompletionListener completionListener) throws JMSException {
        send(destination, message, deliveryMode, priority, timeToLive,
                completionListener);
    }

    @Override
    public void send(Destination destination, Message message,
                     int deliveryMode, int priority, long timeToLive,
                     CompletionListener completionListener) throws JMSException {

        if (completionListener == null) {
            throw new IllegalArgumentException("CompletetionListener cannot be null");
        }
        // Send each message and log message content and ID when successfully received
        MessageId msgId = null;
        try {
            if (message == null) {
                throw new MessageFormatException("Null message");
            }

            message.setJMSReplyTo(DestinationUtils.transformDestination(destination));
            message.setJMSDestination(DestinationUtils.transformDestination(destination));
            message.setJMSTimestamp((new Date()).getTime());
            message.setJMSPriority(priority);

            if (timeToLive > 0) {
                message.setJMSExpiration(System.currentTimeMillis() + timeToLive);
            } else {
                message.setJMSExpiration(0);
            }

            Message transformedMessage = MessageUtils.transformMessage(message);
            if (transformedMessage != null) {
                message = transformedMessage;
            }
            msgId = producer.send(new ObjectSerializer().objectToByteArray(message));
            message.setJMSMessageID(msgId.toString());
        } catch (PulsarClientException e) {
            LOGGER.error("PulsarClientException during send : ", e);
        }
        LOGGER.info("Published msg='{}' with msg-id={}", message.getBody(message.getJMSType().getClass()), msgId);
    }

    @Override
    public Topic getTopic() throws JMSException {
        return null;
    }

    @Override
    public void publish(Message message) throws JMSException {
        this.publish((Topic) this.destination, message, this.getDeliveryMode(), this.getPriority(), this.getTimeToLive());
    }

    @Override
    public void publish(Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        this.publish((Topic) this.destination, message, deliveryMode, priority, timeToLive);
    }

    @Override
    public void publish(Topic topic, Message message) throws JMSException {
        this.publish(topic, message, this.getDeliveryMode(), this.getPriority(), this.getTimeToLive());
    }

    @Override
    public void publish(Topic topic, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException {
        this.send((Destination) topic, message, deliveryMode, priority, timeToLive);
    }
}
