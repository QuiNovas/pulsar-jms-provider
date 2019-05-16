package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.DestinationUtils;
import com.echostreams.pulsar.jms.utils.MessageConverterUtils;
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
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

public class PulsarJMSProducer implements JMSProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSProducer.class);

    private CompletionListener completionListener;
    private PulsarJMSContext jmsContext;
    private Producer<byte[]> producer;

    // Producer send configuration
    private long deliveryDelay = Message.DEFAULT_DELIVERY_DELAY;
    private int deliveryMode = DeliveryMode.PERSISTENT;
    private int priority = Message.DEFAULT_PRIORITY;
    private long timeToLive = Message.DEFAULT_TIME_TO_LIVE;
    private boolean disbledMessageId;
    private boolean disableMessageTimestamp;

    // Message Headers
    private String correlationId;
    private String type;
    private Destination replyTo;
    private byte[] correlationIdBytes;

    // holds the message properties
    private final Map<String, Object> props = new HashMap<String, Object>();
    // read only flag for the message body
    protected boolean readOnlyBody;
    // read only flag for the message properties
    protected boolean readOnlyProperties;

    private final ReentrantLock sendLock = new ReentrantLock();

    public PulsarJMSProducer() {

    }

    public PulsarJMSProducer(PulsarJMSContext jmsContext) throws PulsarClientException, JMSException {
        this.jmsContext = jmsContext;
        this.replyTo = jmsContext.getDestination();
        if (PulsarConfig.producerConfig == null) {
            this.producer = new ProducerBuilderImpl((PulsarClientImpl) jmsContext.getClient(), Schema.BYTES).topic(((PulsarDestination) replyTo).getName()).create();
        } else {
            this.producer = PulsarConfig.producerConfig.topic(((PulsarDestination) replyTo).getName()).create();
        }
    }

    @Override
    public JMSProducer send(Destination destination, Message message) {
        try {
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, String body) {
        try {
            TextMessage message = jmsContext.createTextMessage(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> body) {
        MapMessage message = jmsContext.createMapMessage();
        try {
            for (Map.Entry<String, Object> entry : body.entrySet()) {
                message.setObject(entry.getKey(), entry.getValue());
            }
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, byte[] body) {
        try {
            BytesMessage message = jmsContext.createBytesMessage();
            message.writeBytes(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer send(Destination destination, Serializable body) {
        try {
            ObjectMessage message = jmsContext.createObjectMessage(body);
            sendMessage(destination, message);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setDisableMessageID(boolean disbledMessageId) {
        this.disbledMessageId = disbledMessageId;
        return this;
    }

    @Override
    public boolean getDisableMessageID() {
        return disbledMessageId;
    }

    @Override
    public JMSProducer setDisableMessageTimestamp(boolean disableMessageTimestamp) {
        this.disableMessageTimestamp = disableMessageTimestamp;
        return this;
    }

    @Override
    public boolean getDisableMessageTimestamp() {
        return disableMessageTimestamp;
    }

    @Override
    public JMSProducer setDeliveryMode(int deliveryMode) {
        switch (deliveryMode) {
            case DeliveryMode.PERSISTENT:
            case DeliveryMode.NON_PERSISTENT:
                this.deliveryMode = deliveryMode;
                return this;
            default:
                throw new JMSRuntimeException(String.format("Invalid DeliveryMode specified: %d", deliveryMode));
        }
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
        if (priority < 0 || priority > 9) {
            throw new JMSRuntimeException(String.format("Priority value given {%d} is out of range (0..9)", priority));
        }

        this.priority = priority;
        return this;
    }

    @Override
    public int getPriority() {
        return priority;
    }

    @Override
    public JMSProducer setTimeToLive(long timeToLive) {
        this.timeToLive = timeToLive;
        return this;
    }

    @Override
    public long getTimeToLive() {
        return timeToLive;
    }

    @Override
    public JMSProducer setDeliveryDelay(long deliveryDelay) {
        this.deliveryDelay = deliveryDelay;
        return this;
    }

    @Override
    public long getDeliveryDelay() {
        return deliveryDelay;
    }

    @Override
    public JMSProducer setAsync(CompletionListener completionListener) {
        this.completionListener = completionListener;
        return this;
    }

    @Override
    public CompletionListener getAsync() {
        return completionListener;
    }

    @Override
    public JMSProducer setProperty(String name, boolean value) {
        try {
            setPropertyInternal(name, new Boolean(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        try {
            setPropertyInternal(name, new Byte(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        try {
            setPropertyInternal(name, new Short(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        try {
            setPropertyInternal(name, new Integer(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        try {
            setPropertyInternal(name, new Long(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        try {
            setPropertyInternal(name, new Float(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        try {
            setPropertyInternal(name, new Double(value));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        try {
            setPropertyInternal(name, value);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        try {
            setPropertyInternal(name, value);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public JMSProducer clearProperties() {
        this.readOnlyProperties = false;
        this.props.clear();
        return null;
    }

    @Override
    public boolean propertyExists(String name) {
        return this.props.containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) {
        try {
            return MessageConverterUtils.convertToBoolean(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public byte getByteProperty(String name) {
        try {
            return MessageConverterUtils.convertToByte(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public short getShortProperty(String name) {
        try {
            return MessageConverterUtils.convertToShort(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public int getIntProperty(String name) {
        try {
            return MessageConverterUtils.convertToInt(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public long getLongProperty(String name) {
        try {
            return MessageConverterUtils.convertToLong(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public float getFloatProperty(String name) {
        try {
            return MessageConverterUtils.convertToFloat(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public double getDoubleProperty(String name) {
        try {
            return MessageConverterUtils.convertToDouble(getProperty(name));
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public String getStringProperty(String name) {
        Object value = getProperty(name);
        return value == null ? null : value.toString();
    }

    @Override
    public Object getObjectProperty(String name) {
        return getProperty(name);
    }

    @Override
    public Set<String> getPropertyNames() {
        return new HashSet<String>(this.props.keySet());
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationId) {
        try {
            this.setCorrelationId(MessageConverterUtils.decodeString(correlationId));
            return this;
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        try {
            return MessageConverterUtils.encodeString(this.getCorrelationId());
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
    }

    @Override
    public JMSProducer setJMSCorrelationID(String correlationId) {
        this.setCorrelationId(correlationId);
        return this;
    }

    @Override
    public String getJMSCorrelationID() {
        return this.getCorrelationId();
    }

    @Override
    public JMSProducer setJMSType(String type) {
        this.type = type;
        return this;
    }

    @Override
    public String getJMSType() {
        return type;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination replyTo) {
        try {
            this.replyTo = DestinationUtils.transformDestination(replyTo);
        } catch (JMSException e) {
            throw PulsarJMSException.createRuntimeException(e);
        }
        return this;
    }

    @Override
    public Destination getJMSReplyTo() {
        return replyTo;
    }

    private void setPropertyInternal(String name, Object value)
            throws JMSException {
        if (this.readOnlyProperties) {
            throw new MessageNotWriteableException("properies are read only");
        }
        if (name == null || name.equals("")) {
            throw new IllegalArgumentException("name must not be null or empty");
        }
        this.props.put(name, value);
    }

    private Object getProperty(Object key) {
        return this.props.get(key);
    }

    public String getCorrelationId() {
        return correlationId;
    }

    public void setCorrelationId(String correlationId) {
        this.correlationId = correlationId;
    }

    private void sendMessage(Destination destination, Message message) throws JMSException {
        sendLock.lock();
        // Send each message and log message content and ID when successfully received
        MessageId msgId = null;
        try {
            if (destination == null) {
                throw new InvalidDestinationException("Destination must not be null");
            }

            if (message == null) {
                throw new MessageFormatException("Message must not be null");
            }

            for (Map.Entry<String, Object> entry : props.entrySet()) {
                message.setObjectProperty(entry.getKey(), entry.getValue());
            }

            if (correlationId != null) {
                message.setJMSCorrelationID(correlationId);
            }
            if (correlationIdBytes != null) {
                message.setJMSCorrelationIDAsBytes(correlationIdBytes);
            }
            if (type != null) {
                message.setJMSType(type);
            }
            if (replyTo != null) {
                message.setJMSReplyTo(replyTo);
            }

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
        } finally {
            sendLock.unlock();
        }
        LOGGER.info("Published msg='{}' with msg-id={}", message.getBody(message.getJMSType().getClass()), msgId);
    }
}
