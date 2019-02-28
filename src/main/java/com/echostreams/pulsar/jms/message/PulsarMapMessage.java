package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.utils.CommonUtils;
import com.echostreams.pulsar.jms.utils.MessageConverterUtils;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class PulsarMapMessage extends PulsarMessage implements MapMessage {
    private Map payload;

    public PulsarMapMessage() throws JMSException {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
        setJMSType(PulsarConfig.MAP_MESSAGE);
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        payload.clear();
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) this.payload;
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getBoolean(java.lang.String)
     */
    @Override
    public boolean getBoolean(String name) throws JMSException {
        return MessageConverterUtils.convertToBoolean(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getByte(java.lang.String)
     */
    @Override
    public byte getByte(String name) throws JMSException {
        return MessageConverterUtils.convertToByte(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getShort(java.lang.String)
     */
    @Override
    public short getShort(String name) throws JMSException {
        return MessageConverterUtils.convertToShort(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getChar(java.lang.String)
     */
    @Override
    public char getChar(String name) throws JMSException {
        return MessageConverterUtils.convertToChar(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getInt(java.lang.String)
     */
    @Override
    public int getInt(String name) throws JMSException {
        return MessageConverterUtils.convertToInt(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getLong(java.lang.String)
     */
    @Override
    public long getLong(String name) throws JMSException {
        return MessageConverterUtils.convertToLong(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getFloat(java.lang.String)
     */
    @Override
    public float getFloat(String name) throws JMSException {
        return MessageConverterUtils.convertToFloat(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getDouble(java.lang.String)
     */
    @Override
    public double getDouble(String name) throws JMSException {
        return MessageConverterUtils.convertToDouble(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getString(java.lang.String)
     */
    @Override
    public String getString(String name) throws JMSException {
        return MessageConverterUtils.convertToString(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getBytes(java.lang.String)
     */
    @Override
    public byte[] getBytes(String name) throws JMSException {
        return MessageConverterUtils.convertToBytes(getObject(name));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getObject(java.lang.String)
     */
    @Override
    public Object getObject(String name) throws JMSException {
        if (name == null || name.length() == 0)
            throw new PulsarJMSException("Object name cannot be null or empty", "INVALID_OBJECT_NAME");

        return payload != null ? payload.get(name) : null;
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#getMapNames()
     */
    @Override
    public Enumeration getMapNames() throws JMSException {
        return Collections.enumeration(this.payload.keySet());
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setBoolean(java.lang.String, boolean)
     */
    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name, new Boolean(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setByte(java.lang.String, byte)
     */
    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name, new Byte(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setShort(java.lang.String, short)
     */
    @Override
    public void setShort(String name, short value) throws JMSException {
        put(name, new Short(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setChar(java.lang.String, char)
     */
    @Override
    public void setChar(String name, char value) throws JMSException {
        put(name, new Character(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setInt(java.lang.String, int)
     */
    @Override
    public void setInt(String name, int value) throws JMSException {
        put(name, new Integer(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setLong(java.lang.String, long)
     */
    @Override
    public void setLong(String name, long value) throws JMSException {
        put(name, new Long(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setFloat(java.lang.String, float)
     */
    @Override
    public void setFloat(String name, float value) throws JMSException {
        put(name, new Float(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setDouble(java.lang.String, double)
     */
    @Override
    public void setDouble(String name, double value) throws JMSException {
        put(name, new Double(value));
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setString(java.lang.String, java.lang.String)
     */
    @Override
    public void setString(String name, String value) throws JMSException {
        put(name, value);
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setBytes(java.lang.String, byte[])
     */
    @Override
    public void setBytes(String name, byte[] value) throws JMSException {
        put(name, value != null ? value.clone() : null);
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setBytes(java.lang.String, byte[], int, int)
     */
    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        if (value != null) {
            byte[] reducedValue = new byte[length];
            System.arraycopy(value, offset, reducedValue, 0, length);
            put(name, reducedValue);
        } else
            put(name, null);
    }

    /* (non-Javadoc)
     * @see javax.jms.MapMessage#setObject(java.lang.String, java.lang.Object)
     */
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

    /* (non-Javadoc)
 * @see javax.jms.MapMessage#itemExists(java.lang.String)
 */
    @Override
    public boolean itemExists(String name) throws JMSException {
        if (name == null || name.length() == 0)
            throw new PulsarJMSException("Object name cannot be null or empty", "INVALID_OBJECT_NAME");

        return payload != null ? payload.containsKey(name) : false;
    }


    private Object put(String name, Object value) throws JMSException {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Item name cannot be null");

        checkWriteMode();

        if (payload == null)
            payload = new HashMap<>();

        return payload.put(name, value);
    }

}
