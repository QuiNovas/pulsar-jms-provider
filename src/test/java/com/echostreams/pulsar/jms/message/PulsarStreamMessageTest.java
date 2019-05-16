package com.echostreams.pulsar.jms.message;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.MessageFormatException;

import static org.junit.Assert.*;

public class PulsarStreamMessageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarStreamMessageTest.class);

    @Test
    public void testBooleanConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.readOnlyBody = true;
        assertTrue(msg.readBoolean());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());

        // String
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        assertEquals("true", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeBoolean(true);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertTrue(msg.readBoolean());
    }

    @Test
    public void testByteConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readByte());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        assertEquals(123, msg.readByte());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        assertEquals(123, msg.readShort());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readByte());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        assertEquals(123, msg.readInt());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        assertEquals(123, msg.readLong());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readByte());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readByte());

        // String
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        assertEquals("123", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeByte((byte) 123);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readByte());
    }

    @Test
    public void testShortConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        assertEquals(123, msg.readShort());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        assertEquals(123, msg.readInt());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        assertEquals(123, msg.readLong());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());

        // String
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        assertEquals("123", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeShort((short) 123);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readShort());
    }

    @Test
    public void testCharConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        assertEquals('c', msg.readChar());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());

        // String
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        assertEquals("c", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeChar('c');
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals('c', msg.readChar());
    }

    @Test
    public void testIntConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        assertEquals(123, msg.readInt());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        assertEquals(123, msg.readLong());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());

        // String
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        assertEquals("123", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeInt(123);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readInt());
    }

    @Test
    public void testLongConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        assertEquals(123, msg.readLong());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());

        // String
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        assertEquals("123", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeLong(123);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(123, msg.readLong());
    }

    @Test
    public void testFloatConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Short
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Char
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Int
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Long
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Float
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);

        // Double
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0.0000001);

        // String
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        assertEquals("1.23", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeFloat(1.23f);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23f, msg.readFloat(), 0);
    }

    @Test
    public void testDoubleConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Short
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Char
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Int
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Long
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Float
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);

        // Double
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0.0000001);

        // String
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        assertEquals("1.23", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeDouble(1.23);
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(1.23, msg.readDouble(), 0);
    }

    @Test
    public void testStringConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        assertFalse(msg.readBoolean());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Double
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());

        // String
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        assertEquals("foobar", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeString("foobar");
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("foobar", msg.readString());
    }

    @Test
    public void testNumericStringConversion() throws Exception {
        PulsarStreamMessage msg;

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertFalse(msg.readBoolean());

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals((byte) 123, msg.readByte());

        // Short
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals(123, msg.readShort());

        // Char
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("123", msg.readString());

        // Int
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals(123, msg.readInt());

        // Long
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals(123, msg.readLong());

        // Float
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals(123, msg.readFloat(), 0);

        // Double
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals(123, msg.readDouble(), 0);

        // String
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        assertEquals("123", msg.readString());

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeString("123");
        msg.reset();
        try {
            msg.readBytes(new byte[1]);
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals("123", msg.readString());
    }

    @Test
    public void testBytesConversion() throws Exception {
        PulsarStreamMessage msg;

        byte[] dummy = {(byte) 1, (byte) 2, (byte) 3};

        // Boolean
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readBoolean();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Byte
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readByte();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Short
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readShort();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Char
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readChar();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Int
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readInt();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Long
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readLong();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Float
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readFloat();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Double
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readDouble();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // String
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        try {
            msg.readString();
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */ }
        msg.reset();
        assertEquals(3, msg.readBytes(new byte[3]));

        // Bytes
        msg = new PulsarStreamMessage();
        msg.writeBytes(dummy);
        msg.reset();
        byte[] data = new byte[3];
        assertEquals(3, msg.readBytes(data));
        LOGGER.info("", data[0]);
        assertEquals(1, data[0]);
        assertEquals(2, data[1]);
        assertEquals(3, data[2]);
    }

    @Test
    public void testToString() throws Exception {
        PulsarStreamMessage streamMessage;

        streamMessage = new PulsarStreamMessage();
        assertTrue(streamMessage.toString().startsWith("PulsarStreamMessage"));
    }
}
