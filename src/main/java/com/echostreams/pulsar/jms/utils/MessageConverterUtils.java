package com.echostreams.pulsar.jms.utils;

import javax.jms.JMSException;
import javax.jms.MessageFormatException;
import java.io.UnsupportedEncodingException;

public class MessageConverterUtils {

    public static boolean convertToBoolean(Object value) throws MessageFormatException {
        if (value instanceof Boolean)
            return ((Boolean) value).booleanValue();
        if (value == null || value instanceof String)
            return Boolean.valueOf((String) value).booleanValue();

        throw new MessageFormatException("Could not convert type to boolean : (" + value.getClass().getName() + ") " + value);
    }

    public static byte convertToByte(Object value) throws MessageFormatException {
        if (value instanceof Byte)
            return ((Byte) value).byteValue();
        if (value == null || value instanceof String)
            return Byte.valueOf((String) value).byteValue();

        throw new MessageFormatException("Could not convert type to byte : (" + value.getClass().getName() + ") " + value);
    }

    public static short convertToShort(Object value) throws MessageFormatException {
        if (value == null || value instanceof String)
            return Short.valueOf((String) value).shortValue();
        if (value instanceof Short)
            return ((Short) value).shortValue();
        if (value instanceof Byte)
            return ((Byte) value).byteValue();

        throw new MessageFormatException("Could not convert type to short : (" + value.getClass().getName() + ") " + value);
    }

    public static char convertToChar(Object value) throws MessageFormatException {
        if (value == null)
            throw new NullPointerException(); // [JMS Spec]
        if (value instanceof Character)
            return ((Character) value).charValue();

        throw new MessageFormatException("Could not convert type to char : (" + value.getClass().getName() + ") " + value);
    }

    public static int convertToInt(Object value) throws MessageFormatException {
        if (value == null)
            return Integer.valueOf((String) null).intValue();
        if (value instanceof Integer)
            return ((Integer) value).intValue();
        if (value instanceof Byte)
            return ((Byte) value).byteValue();
        if (value instanceof Short)
            return ((Short) value).shortValue();
        if (value instanceof String)
            return Integer.valueOf((String) value).intValue();

        throw new MessageFormatException("Could not convert type to int : (" + value.getClass().getName() + ") " + value);
    }

    public static long convertToLong(Object value) throws MessageFormatException {
        if (value == null)
            return Long.valueOf((String) null).longValue();

        if (value instanceof Long)
            return ((Long) value).longValue();
        if (value instanceof Byte)
            return ((Byte) value).byteValue();
        if (value instanceof Short)
            return ((Short) value).shortValue();
        if (value instanceof Integer)
            return ((Integer) value).intValue();
        if (value instanceof String)
            return Long.valueOf((String) value).longValue();

        throw new MessageFormatException("Could not convert type to long : (" + value.getClass().getName() + ") " + value);
    }

    public static float convertToFloat(Object value) throws MessageFormatException {
        if (value == null)
            throw new NullPointerException();
        // Same as "return Float.valueOf((String)null).floatValue();" (JMS Spec)

        if (value instanceof Float)
            return ((Float) value).floatValue();
        if (value instanceof String)
            return Float.valueOf((String) value).floatValue();

        throw new MessageFormatException("Could not convert type to float : (" + value.getClass().getName() + ") " + value);
    }

    public static double convertToDouble(Object value) throws MessageFormatException {
        if (value == null)
            throw new NullPointerException();
        // Same as "return Double.valueOf((String)null).doubleValue();" (JMS Spec)

        if (value instanceof Double)
            return ((Double) value).doubleValue();
        if (value instanceof Float)
            return ((Float) value).floatValue();
        if (value instanceof String)
            return Double.valueOf((String) value).doubleValue();

        throw new MessageFormatException("Could not convert type to double : (" + value.getClass().getName() + ") " + value);
    }

    public static String convertToString(Object value) throws MessageFormatException {
        if (value == null)
            return null;

        if (value instanceof String)
            return (String) value;

        if (value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Character ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof Character)
            return String.valueOf(value);

        throw new MessageFormatException("Could not convert type to String : (" + value.getClass().getName() + ") " + value);
    }

    public static byte[] convertToBytes(Object value) throws MessageFormatException {
        if (value == null)
            return null;

        if (value instanceof byte[])
            return CommonUtils.copy((byte[]) value);

        throw new MessageFormatException("Could not convert type to byte[] : (" + value.getClass().getName() + ") " + value);
    }

    public static String decodeString(byte[] data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

    public static byte[] encodeString(String data) throws JMSException {
        try {
            if (data == null) {
                return null;
            }
            return data.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new JMSException("Invalid UTF-8 encoding: " + e.getMessage());
        }
    }

}
