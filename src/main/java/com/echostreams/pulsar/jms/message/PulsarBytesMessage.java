package com.echostreams.pulsar.jms.message;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import java.io.Serializable;
import java.util.HashMap;

public class PulsarBytesMessage extends PulsarMessage implements BytesMessage {
    private byte[] payload;

    public PulsarBytesMessage() {
        headers = new HashMap<>();
        headers.put(PROPERTIES, new HashMap<String, Serializable>());
    }

    /* (non-Javadoc)
     * @see javax.jms.Message#clearBody()
     */
    @Override
    public void clearBody() throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Message#getBody(java.lang.Class)
     */
    @Override
    public <T> T getBody(Class<T> c) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#getBodyLength()
     */
    @Override
    public long getBodyLength() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readBoolean()
     */
    @Override
    public boolean readBoolean() throws JMSException {
        // TODO Auto-generated method stub
        return false;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readByte()
     */
    @Override
    public byte readByte() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readUnsignedByte()
     */
    @Override
    public int readUnsignedByte() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readShort()
     */
    @Override
    public short readShort() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readUnsignedShort()
     */
    @Override
    public int readUnsignedShort() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readChar()
     */
    @Override
    public char readChar() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readInt()
     */
    @Override
    public int readInt() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readLong()
     */
    @Override
    public long readLong() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readFloat()
     */
    @Override
    public float readFloat() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readDouble()
     */
    @Override
    public double readDouble() throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readUTF()
     */
    @Override
    public String readUTF() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readBytes(byte[])
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#readBytes(byte[], int)
     */
    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeBoolean(boolean)
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeByte(byte)
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeShort(short)
     */
    @Override
    public void writeShort(short value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeChar(char)
     */
    @Override
    public void writeChar(char value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeInt(int)
     */
    @Override
    public void writeInt(int value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeLong(long)
     */
    @Override
    public void writeLong(long value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeFloat(float)
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeDouble(double)
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeUTF(java.lang.String)
     */
    @Override
    public void writeUTF(String value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeBytes(byte[])
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeBytes(byte[], int, int)
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length)
            throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#writeObject(java.lang.Object)
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.BytesMessage#reset()
     */
    @Override
    public void reset() throws JMSException {
        // TODO Auto-generated method stub

    }

}
