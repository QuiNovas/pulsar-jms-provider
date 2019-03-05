package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.utils.MessageConverterUtils;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.ProducerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.Serializable;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;

public class PulsarJMSProducer implements JMSProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSProducer.class);

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
    /* holds the message properties */
    private Hashtable props;
    private String correlationId;
    /* read only flag for the message body */
    protected boolean readOnlyBody;
    /* read only flag for the message properties */
    protected boolean readOnlyProperties;
    private CompletionListener completionListener;


    public PulsarJMSProducer(PulsarConnection connection) throws PulsarClientException, JMSException {
        this.destination = (PulsarDestination) destination;
        //TODO need to map with pulsar producer config
        this.producer = new ProducerBuilderImpl((PulsarClientImpl) connection.getClient(), Schema.BYTES).topic(((PulsarDestination) destination).getName()).create();
    }


    @Override
    public JMSProducer send(Destination destination, Message message) {
        // send(destination, message, deliveryMode, priority, timeToLive);
        return null;
    }

    @Override
    public JMSProducer send(Destination destination, String messsage) {
        // producer.send(messsage);
        return null;
    }

    @Override
    public JMSProducer send(Destination destination, Map<String, Object> map) {
        return null;
    }

    @Override
    public JMSProducer send(Destination destination, byte[] bytes) {
        return null;
    }

    @Override
    public JMSProducer send(Destination destination, Serializable serializable) {
        return null;
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
        this.deliveryMode = deliveryMode;
        return this;
    }

    @Override
    public int getDeliveryMode() {
        return deliveryMode;
    }

    @Override
    public JMSProducer setPriority(int priority) {
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
            LOGGER.error("Set boolean exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, byte value) {
        try {
            setPropertyInternal(name, new Byte(value));
        } catch (JMSException e) {
            LOGGER.error("Set byte exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, short value) {
        try {
            setPropertyInternal(name, new Short(value));
        } catch (JMSException e) {
            LOGGER.error("Set short exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, int value) {
        try {
            setPropertyInternal(name, new Integer(value));
        } catch (JMSException e) {
            LOGGER.error("Set int exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, long value) {
        try {
            setPropertyInternal(name, new Long(value));
        } catch (JMSException e) {
            LOGGER.error("Set long exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, float value) {
        try {
            setPropertyInternal(name, new Float(value));
        } catch (JMSException e) {
            LOGGER.error("Set float exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, double value) {
        try {
            setPropertyInternal(name, new Double(value));
        } catch (JMSException e) {
            LOGGER.error("Set double exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, String value) {
        try {
            setPropertyInternal(name, value);
        } catch (JMSException e) {
            LOGGER.error("Set String exception", e);
        }
        return this;
    }

    @Override
    public JMSProducer setProperty(String name, Object value) {
        try {
            setPropertyInternal(name, value);
        } catch (JMSException e) {
            LOGGER.error("Set Object exception", e);
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
        } catch (MessageFormatException e) {
            LOGGER.error("getBooleanProperty exception", e);
        }
        return false;
    }

    @Override
    public byte getByteProperty(String name) {
        try {
            return MessageConverterUtils.convertToByte(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getByteProperty exception", e);
        }
        return 0;
    }

    @Override
    public short getShortProperty(String name) {
        try {
            return MessageConverterUtils.convertToShort(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getShortProperty exception", e);
        }
        return 0;
    }

    @Override
    public int getIntProperty(String name) {
        try {
            return MessageConverterUtils.convertToInt(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getIntProperty exception", e);
        }
        return 0;
    }

    @Override
    public long getLongProperty(String name) {
        try {
            return MessageConverterUtils.convertToLong(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getLongProperty exception", e);
        }
        return 0;
    }

    @Override
    public float getFloatProperty(String name) {
        try {
            return MessageConverterUtils.convertToFloat(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getFloatProperty exception", e);
        }
        return 0;
    }

    @Override
    public double getDoubleProperty(String name) {
        try {
            return MessageConverterUtils.convertToDouble(getProperty(name));
        } catch (MessageFormatException e) {
            LOGGER.error("getDoubleProperty exception", e);
        }
        return 0;
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
        return this.props.keySet();
    }

    @Override
    public JMSProducer setJMSCorrelationIDAsBytes(byte[] correlationId) {
        try {
            this.setCorrelationId(MessageConverterUtils.decodeString(correlationId));
        } catch (JMSException e) {
            LOGGER.error("setJMSCorrelationIDAsBytes exception", e);
        }
        return this;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() {
        try {
            return MessageConverterUtils.encodeString(this.getCorrelationId());
        } catch (JMSException e) {
            LOGGER.error("getJMSCorrelationIDAsBytes exception", e);
        }
        return new byte[0];
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
    public JMSProducer setJMSType(String s) {
        return null;
    }

    @Override
    public String getJMSType() {
        return null;
    }

    @Override
    public JMSProducer setJMSReplyTo(Destination destination) {
        return null;
    }

    @Override
    public Destination getJMSReplyTo() {
        return null;
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
}
