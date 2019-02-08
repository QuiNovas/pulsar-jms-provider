package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.common.AbstractMessage;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageNotReadableException;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

public class PulsarBytesMessage extends AbstractMessage implements BytesMessage {

    private byte[] body;
    private transient DataInputStream input;
    private transient DataOutputStream output;
    private transient ByteArrayInputStream inputBuf;
    private transient ByteArrayOutputStream outputBuf;

    public PulsarBytesMessage() { super(); }

    @Override
    public long getBodyLength() throws JMSException {
        if (!bodyIsReadOnly)
            throw new MessageNotReadableException("Message body is write-only");

        return body != null ? body.length : 0;
    }

    @Override
    public boolean readBoolean() throws JMSException {
        return false;
    }

    @Override
    public byte readByte() throws JMSException {
        return 0;
    }

    @Override
    public int readUnsignedByte() throws JMSException {
        return 0;
    }

    @Override
    public short readShort() throws JMSException {
        return 0;
    }

    @Override
    public int readUnsignedShort() throws JMSException {
        return 0;
    }

    @Override
    public char readChar() throws JMSException {
        return 0;
    }

    @Override
    public int readInt() throws JMSException {
        return 0;
    }

    @Override
    public long readLong() throws JMSException {
        return 0;
    }

    @Override
    public float readFloat() throws JMSException {
        return 0;
    }

    @Override
    public double readDouble() throws JMSException {
        return 0;
    }

    @Override
    public String readUTF() throws JMSException {
        return null;
    }

    @Override
    public int readBytes(byte[] bytes) throws JMSException {
        return 0;
    }

    @Override
    public int readBytes(byte[] bytes, int i) throws JMSException {
        return 0;
    }

    @Override
    public void writeBoolean(boolean b) throws JMSException {

    }

    @Override
    public void writeByte(byte b) throws JMSException {

    }

    @Override
    public void writeShort(short i) throws JMSException {

    }

    @Override
    public void writeChar(char c) throws JMSException {

    }

    @Override
    public void writeInt(int i) throws JMSException {

    }

    @Override
    public void writeLong(long l) throws JMSException {

    }

    @Override
    public void writeFloat(float v) throws JMSException {

    }

    @Override
    public void writeDouble(double v) throws JMSException {

    }

    @Override
    public void writeUTF(String s) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] bytes) throws JMSException {

    }

    @Override
    public void writeBytes(byte[] bytes, int i, int i1) throws JMSException {

    }

    @Override
    public void writeObject(Object o) throws JMSException {

    }

    @Override
    public void reset() throws JMSException {

    }
}
