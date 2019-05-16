package com.echostreams.pulsar.jms.message;

import org.junit.Test;

import javax.jms.MessageFormatException;

import static org.junit.Assert.*;

public class PulsarMapMessageTest {

    @Test
    public void testBooleanConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        assertTrue(msg.getBoolean("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        assertEquals("true", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setBoolean("prop", true);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertTrue(msg.getBoolean("prop"));
    }

    @Test
    public void testByteConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getByte("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        assertEquals(123, msg.getByte("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        assertEquals(123, msg.getShort("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getByte("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        assertEquals(123, msg.getInt("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        assertEquals(123, msg.getLong("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getByte("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getByte("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        assertEquals("123", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setByte("prop", (byte) 123);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getByte("prop"));
    }

    @Test
    public void testShortConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        assertEquals(123, msg.getShort("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        assertEquals(123, msg.getInt("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        assertEquals(123, msg.getLong("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        assertEquals("123", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setShort("prop", (short) 123);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getShort("prop"));
    }

    @Test
    public void testCharConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        assertEquals('c', msg.getChar("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        assertEquals("c", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setChar("prop", 'c');
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals('c', msg.getChar("prop"));
    }

    @Test
    public void testIntConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        assertEquals(123, msg.getInt("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        assertEquals(123, msg.getLong("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        assertEquals("123", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setInt("prop", 123);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getInt("prop"));
    }

    @Test
    public void testLongConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        assertEquals(123, msg.getLong("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        assertEquals("123", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setLong("prop", 123);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(123, msg.getLong("prop"));
    }

    @Test
    public void testFloatConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Byte
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Short
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Char
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Int
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Long
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Float
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        assertEquals(1.23f, msg.getFloat("prop"), 0);

        // Double
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        assertEquals(1.23, msg.getDouble("prop"), 0.0000001);

        // String
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        assertEquals("1.23", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setFloat("prop", 1.23f);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23f, msg.getFloat("prop"), 0);
    }

    @Test
    public void testDoubleConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Byte
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Short
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Char
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Int
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Long
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Float
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);

        // Double
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        assertEquals(1.23, msg.getDouble("prop"), 0.0000001);

        // String
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        assertEquals("1.23", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setDouble("prop", 1.23);
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(1.23, msg.getDouble("prop"), 0);
    }

    @Test
    public void testStringConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        assertFalse(msg.getBoolean("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // Double
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (NumberFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));

        // String
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        assertEquals("foobar", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setString("prop", "foobar");
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals("foobar", msg.getString("prop"));
    }

    @Test
    public void testNumericStringConversion() throws Exception {
        PulsarMapMessage msg;

        // Boolean
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertFalse(msg.getBoolean("prop"));

        // Byte
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals((byte) 123, msg.getByte("prop"));

        // Short
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals(123, msg.getShort("prop"));

        // Char
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals("123", msg.getString("prop"));

        // Int
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals(123, msg.getInt("prop"));

        // Long
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals(123, msg.getLong("prop"));

        // Float
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals(123, msg.getFloat("prop"), 0);

        // Double
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals(123, msg.getDouble("prop"), 0);

        // String
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        assertEquals("123", msg.getString("prop"));

        // Bytes
        msg = new PulsarMapMessage();
        msg.setString("prop", "123");
        try {
            msg.getBytes("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals("123", msg.getString("prop"));
    }

    @Test
    public void testBytesConversion() throws Exception {
        PulsarMapMessage msg;

        byte[] dummy = {(byte) 1, (byte) 2, (byte) 3};

        // Boolean
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getBoolean("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Byte
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getByte("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Short
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getShort("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Char
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getChar("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Int
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getInt("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Long
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getLong("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Float
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getFloat("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Double
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getDouble("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // String
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        try {
            msg.getString("prop");
            fail("Should have failed");
        } catch (MessageFormatException e) { /* OK */}
        assertEquals(3, msg.getBytes("prop").length);

        // Bytes
        msg = new PulsarMapMessage();
        msg.setBytes("prop", dummy);
        byte[] data = msg.getBytes("prop");
        assertEquals(3, data.length);
        assertEquals(1, data[0]);
        assertEquals(2, data[1]);
        assertEquals(3, data[2]);
    }

    @Test
    public void testToString() throws Exception {
        PulsarMapMessage mapMessage;

        mapMessage = new PulsarMapMessage();
        assertTrue(mapMessage.toString().startsWith("PulsarMapMessage"));
    }
}
