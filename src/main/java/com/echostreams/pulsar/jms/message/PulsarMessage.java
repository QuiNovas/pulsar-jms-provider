package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.JmsHeaderKeys;
import com.echostreams.pulsar.jms.client.PulsarDestination;

import javax.annotation.PostConstruct;
import javax.jms.*;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class PulsarMessage implements Message, Serializable {

    private static final long serialVersionUID = -1717904123537181827L;

    public static final String PROPERTIES = "properties";
    protected Map<String, Serializable> headers;
    /* read only flag for the message body */
    protected boolean readOnlyBody;

    public PulsarMessage() {
        super();
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
    }

    @PostConstruct
    protected void verify() {
        if (headers == null) {
            headers = new HashMap<String, Serializable>();
        }
        if (headers.get(PROPERTIES) == null) {
            headers.put(PROPERTIES, new HashMap<String, Serializable>());
        }
    }

    @Override
    public String getJMSMessageID() throws JMSException {
        return (String) headers.get(JmsHeaderKeys.JMSMessageID);
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        headers.put(JmsHeaderKeys.JMSMessageID, id);

    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        return (Long) headers.get(JmsHeaderKeys.JMSTimestamp);
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        headers.put(JmsHeaderKeys.JMSTimestamp, timestamp);

    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return ((String) headers.get(JmsHeaderKeys.JMSCorrelationID)).getBytes();
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] correlationID)
            throws JMSException {
        headers.put(JmsHeaderKeys.JMSCorrelationID, new String(correlationID));

    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        headers.put(JmsHeaderKeys.JMSCorrelationID, correlationID);
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        return (String) headers.get((String) JmsHeaderKeys.JMSCorrelationID);
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        return (Destination) headers.get(JmsHeaderKeys.JMSReplyTo);
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        headers.put(JmsHeaderKeys.JMSReplyTo, (PulsarDestination) replyTo);
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return (Destination) headers.get(JmsHeaderKeys.JMSDestination);
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        headers.put(JmsHeaderKeys.JMSDestination, (PulsarDestination) destination);
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return (Integer) headers.get(JmsHeaderKeys.JMSDeliveryMode);
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        headers.put(JmsHeaderKeys.JMSDeliveryMode, deliveryMode);
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return (Boolean) headers.get(JmsHeaderKeys.JMSRedelivered);
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        headers.put(JmsHeaderKeys.JMSRedelivered, redelivered);
    }

    @Override
    public String getJMSType() throws JMSException {
        return (String) headers.get(JmsHeaderKeys.JMSType);
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        headers.put(JmsHeaderKeys.JMSType, type);
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return (Long) headers.get(JmsHeaderKeys.JMSExpiration);
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        headers.put(JmsHeaderKeys.JMSExpiration, expiration);
    }

    @Override
    public long getJMSDeliveryTime() throws JMSException {
        return (Long) headers.get(JmsHeaderKeys.JMSDeliveryTime);
    }

    @Override
    public void setJMSDeliveryTime(long deliveryTime) throws JMSException {
        headers.put(JmsHeaderKeys.JMSDeliveryTime, deliveryTime);
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return (Integer) headers.get(JmsHeaderKeys.JMSPriority);
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        headers.put(JmsHeaderKeys.JMSPriority, priority);
    }

    @Override
    public void clearProperties() throws JMSException {
        ((Map) headers.get(PROPERTIES)).clear();
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        return ((Map) headers.get(PROPERTIES)).containsKey(name);
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return (Boolean) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return (Byte) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return (Short) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return (Integer) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return (Long) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return (Float) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return (Double) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return (String) ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return ((Map) headers.get(PROPERTIES)).get(name);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return Collections.enumeration(((Map) headers.get(PROPERTIES)).keySet());
    }

    @Override
    public void setBooleanProperty(String name, boolean value)
            throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setDoubleProperty(String name, double value)
            throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setStringProperty(String name, String value)
            throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value)
            throws JMSException {
        ((Map) headers.get(PROPERTIES)).put(name, value);
    }

    @Override
    public void acknowledge() throws JMSException {
        // TODO Auto-generated method stub

    }

    @Override
    public void clearBody() throws JMSException {
        checkWriteMode();
        readOnlyBody = false;
    }

    @Override
    public <T> T getBody(Class<T> aClass) throws JMSException {
        if (isBodyAssignableTo(aClass)) {
            return doGetBody(aClass);
        }

        throw new MessageFormatException("Message body cannot be read as type: " + aClass);
    }

    protected <T> T doGetBody(Class<T> aClass) throws JMSException {
        return null;
    }

    @Override
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        return c.getName().equals(String.class.getName());
    }

    public final void checkWriteMode() throws MessageNotWriteableException {
        if (this.readOnlyBody) {
            throw new MessageNotWriteableException("Message in read-only mode");
        }
    }

    public final void checkReadMode() throws MessageNotReadableException {
        if (!this.readOnlyBody) {
            throw new MessageNotReadableException("Message in write-only mode");
        }
    }

    @Override
    public String toString() {
        return "PulsarMessage{" +
                "headers=" + headers +
                ", readOnlyBody=" + readOnlyBody +
                '}';
    }
}
