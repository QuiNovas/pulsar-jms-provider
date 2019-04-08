package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.CommonUtils;
import com.echostreams.pulsar.jms.utils.MessageConverterUtils;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class PulsarMapMessage extends PulsarMessage implements MapMessage {

    private static final long serialVersionUID = -4895466833333293321L;

    private Map payload;

    public PulsarMapMessage() throws JMSException {
        super();
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConstants.MAP_MESSAGE);
    }

    @Override
    public void clearBody() throws JMSException {
        payload.clear();
    }

    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) this.payload;
    }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return MessageConverterUtils.convertToBoolean(getObject(name));
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return MessageConverterUtils.convertToByte(getObject(name));
    }

    @Override
    public short getShort(String name) throws JMSException {
        return MessageConverterUtils.convertToShort(getObject(name));
    }

    @Override
    public char getChar(String name) throws JMSException {
        return MessageConverterUtils.convertToChar(getObject(name));
    }

    @Override
    public int getInt(String name) throws JMSException {
        return MessageConverterUtils.convertToInt(getObject(name));
    }

    @Override
    public long getLong(String name) throws JMSException {
        return MessageConverterUtils.convertToLong(getObject(name));
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return MessageConverterUtils.convertToFloat(getObject(name));
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return MessageConverterUtils.convertToDouble(getObject(name));
    }

    @Override
    public String getString(String name) throws JMSException {
        return MessageConverterUtils.convertToString(getObject(name));
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return MessageConverterUtils.convertToBytes(getObject(name));
    }

    @Override
    public Object getObject(String name) throws JMSException {
        if (name == null || name.length() == 0)
            throw new PulsarJMSException("Object name cannot be null or empty", "INVALID_OBJECT_NAME");

        return payload != null ? payload.get(name) : null;
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        return Collections.enumeration(this.payload.keySet());
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setShort(String name, short value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setChar(String name, char value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setInt(String name, int value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setLong(String name, long value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setFloat(String name, float value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setDouble(String name, double value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setString(String name, String value) throws JMSException {
        put(name, value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        put(name, value != null ? value.clone() : null);
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        if (value != null) {
            byte[] reducedValue = new byte[length];
            System.arraycopy(value, offset, reducedValue, 0, length);
            put(name, reducedValue);
        } else
            put(name, null);
    }

    @Override
    public void setObject(String name, Object value) throws JMSException {
        if (value != null) {
            if (!(value instanceof Boolean ||
                    value instanceof Byte ||
                    value instanceof Character ||
                    value instanceof Short ||
                    value instanceof Integer ||
                    value instanceof Long ||
                    value instanceof Float ||
                    value instanceof Double ||
                    value instanceof String ||
                    value instanceof byte[]))
                throw new MessageFormatException("Unsupported value type : " + value.getClass().getName());

            if (value instanceof byte[])
                value = CommonUtils.copy((byte[]) value); // [JMS Spec]

            put(name, value);
        }
    }

    @Override
    public boolean itemExists(String name) throws JMSException {
        if (name == null || name.length() == 0)
            throw new PulsarJMSException("Object name cannot be null or empty", "INVALID_OBJECT_NAME");

        return payload != null && payload.containsKey(name);
    }

    private Object put(String name, Object value) throws JMSException {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Item name cannot be null");

        checkWriteMode();

        if (payload == null)
            payload = new HashMap<>();

        return payload.put(name, value);
    }

    @Override
    public String toString() {
        return "PulsarMapMessage{" +
                "payload=" + payload +
                '}';
    }
}
