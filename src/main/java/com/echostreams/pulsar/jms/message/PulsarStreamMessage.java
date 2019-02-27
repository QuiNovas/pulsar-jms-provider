package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.config.PulsarConfig;

import javax.jms.JMSException;
import javax.jms.StreamMessage;
import java.io.Serializable;
import java.util.HashMap;
import java.util.stream.Stream;

public class PulsarStreamMessage extends PulsarMessage implements StreamMessage {
    private Stream payload;

    /**
     *
     */
    public PulsarStreamMessage() throws JMSException {
        setJMSType(PulsarConfig.STREAM_MESSAGE);
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        payload = null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        return (T) payload;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readBoolean()
     */
    @Override
    public boolean readBoolean() throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readByte()
     */
    @Override
    public byte readByte() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readShort()
     */
    @Override
    public short readShort() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readChar()
     */
    @Override
    public char readChar() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readInt()
     */
    @Override
    public int readInt() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readLong()
     */
    @Override
    public long readLong() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readFloat()
     */
    @Override
    public float readFloat() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readDouble()
     */
    @Override
    public double readDouble() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readString()
     */
    @Override
    public String readString() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readBytes(byte[])
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#readObject()
     */
    @Override
    public Object readObject() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeByte(byte)
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeShort(short)
     */
    @Override
    public void writeShort(short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeChar(char)
     */
    @Override
    public void writeChar(char value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeInt(int)
     */
    @Override
    public void writeInt(int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeLong(long)
     */
    @Override
    public void writeLong(long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeFloat(float)
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeDouble(double)
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeString(java.lang.String)
     */
    @Override
    public void writeString(String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBytes(byte[])
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeBytes(byte[], int, int)
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length)
            throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#writeObject(java.lang.Object)
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.StreamMessage#reset()
     */
    @Override
    public void reset() throws JMSException {
        // TODO Auto-generated method stub

    }

}
