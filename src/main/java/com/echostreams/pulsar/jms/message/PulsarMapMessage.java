package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;
import com.echostreams.pulsar.jms.common.MessageConverter;
import com.echostreams.pulsar.jms.common.MessageGroup;
import com.echostreams.pulsar.jms.common.MessageSerializationLevel;
import com.echostreams.pulsar.jms.utils.*;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class PulsarMapMessage extends AbstractMessage implements MapMessage {

    private Map<String,Object> body;

    public PulsarMapMessage(){ super(); }

    @Override
    public boolean getBoolean(String name) throws JMSException {
        return MessageConverter.asBoolean(getObject(name));
    }

    @Override
    public byte getByte(String name) throws JMSException {
        return MessageConverter.asByte(getObject(name));
    }

    @Override
    public short getShort(String name) throws JMSException {
        return MessageConverter.asShort(getObject(name));
    }

    @Override
    public char getChar(String name) throws JMSException {
        return MessageConverter.asChar(getObject(name));
    }

    @Override
    public int getInt(String name) throws JMSException {
        return MessageConverter.asInt(getObject(name));
    }

    @Override
    public long getLong(String name) throws JMSException {
        return MessageConverter.asLong(getObject(name));
    }

    @Override
    public float getFloat(String name) throws JMSException {
        return MessageConverter.asFloat(getObject(name));
    }

    @Override
    public double getDouble(String name) throws JMSException {
        return MessageConverter.asDouble(getObject(name));
    }

    @Override
    public String getString(String name) throws JMSException {
        return MessageConverter.asString(getObject(name));
    }

    @Override
    public byte[] getBytes(String name) throws JMSException {
        return MessageConverter.asBytes(getObject(name));
    }

    @Override
    public Object getObject(String name) throws JMSException {
        if (StringRelatedUtils.isEmpty(name))
            throw new PulsarJMSException("Object name cannot be null or empty","INVALID_OBJECT_NAME");

        return body != null ? body.get(name) : null;
    }

    @Override
    public Enumeration getMapNames() throws JMSException {
        if (body == null)
            return new EmptyEnumeration<>();

        return new IteratorEnumeration<>(body.keySet().iterator());
    }

    @Override
    public void setBoolean(String name, boolean value) throws JMSException {
        put(name,Boolean.valueOf(value));
    }

    @Override
    public void setByte(String name, byte value) throws JMSException {
        put(name,Byte.valueOf(value));
    }

    @Override
    public void setShort(String name, short value) throws JMSException
    {
        put(name,Short.valueOf(value));
    }

    @Override
    public void setChar(String name, char value) throws JMSException
    {
        put(name,new Character(value));
    }

    @Override
    public void setInt(String name, int value) throws JMSException
    {
        put(name,Integer.valueOf(value));
    }

    @Override
    public void setLong(String name, long value) throws JMSException
    {
        put(name,Long.valueOf(value));
    }

    @Override
    public void setFloat(String name, float value) throws JMSException
    {
        put(name,new Float(value));
    }

    @Override
    public void setDouble(String name, double value) throws JMSException
    {
        put(name,new Double(value));
    }

    @Override
    public void setString(String name, String value) throws JMSException
    {
        put(name,value);
    }

    @Override
    public void setBytes(String name, byte[] value) throws JMSException
    {
        put(name,value != null ? value.clone() : null);
    }

    @Override
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException
    {
        if (value != null)
        {
            byte[] reducedValue = new byte[length];
            System.arraycopy(value, offset, reducedValue, 0, length);
            put(name,reducedValue);
        }
        else
            put(name,null);
    }

    @Override
    public void setObject(String name, Object value) throws JMSException
    {
        if (value != null)
        {
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
                throw new MessageFormatException("Unsupported value type : "+value.getClass().getName());

            if (value instanceof byte[])
                value = ArrayRelatedUtils.copy((byte[]) value); // [JMS Spec]

            put(name,value);
        }
        else
            put(name,null);
    }

    @Override
    public boolean itemExists(String name) throws JMSException
    {
        if (name == null || name.length() == 0)
            throw new PulsarJMSException("Object name cannot be null or empty","INVALID_OBJECT_NAME");

        return body != null ? body.containsKey(name) : false;
    }

    @Override
    protected byte getType() {
        return MessageGroup.MAP;
    }

    @Override
    public AbstractMessage copy() {
        PulsarMapMessage clone = new PulsarMapMessage();
        copyCommonFields(clone);
        if (this.body != null)
        {
            clone.body = new HashMap<>();
            clone.body.putAll(this.body);
        }

        return clone;
    }

    @Override
    protected void serializeBodyTo(RawDataBuffer out) {

    }

    @Override
    protected void unserializeBodyFrom(RawDataBuffer in) {

    }

    @Override
    public void clearBody() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        if (body != null) body.clear();
        bodyIsReadOnly = false;
    }

    private Object put(String name,Object value) throws JMSException
    {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Item name cannot be null");

        if (bodyIsReadOnly)
            throw new MessageNotWriteableException("Message body is read-only");

        assertDeserializationLevel(MessageSerializationLevel.FULL);
        if (body == null)
            body = new HashMap<>();

        return body.put(name,value);
    }
}
