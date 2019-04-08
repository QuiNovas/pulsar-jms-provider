package com.echostreams.pulsar.jms.utils;

import com.echostreams.pulsar.jms.message.*;

import javax.jms.*;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility functions to copy and normalize JMS messages.</p>
 */
public final class MessageUtils {

    /**
     * Map holding all valid class/type pairs
     */
    private static Map<Object, Object> object2Type;

    /**
     * initilizes the object2Type map.
     */
    static {
        object2Type = new HashMap<>(8);
        object2Type.put(Boolean.class, "boolean");
        object2Type.put(Byte.class, "byte");
        object2Type.put(byte[].class, "byte[]");
        object2Type.put(Short.class, "short");
        object2Type.put(Integer.class, "int");
        object2Type.put(Long.class, "long");
        object2Type.put(Float.class, "float");
        object2Type.put(Double.class, "double");
        object2Type.put(String.class, "string");
        object2Type.put(Character.class, "char");
    }

    public static PulsarMessage transformMessage(Message sourceMsg) throws JMSException {
        PulsarMessage msgCopy;

        if (sourceMsg instanceof TextMessage) {
            msgCopy = duplicateTextMessage((TextMessage) sourceMsg);
        } else if (sourceMsg instanceof BytesMessage) {
            msgCopy = duplicateBytesMessage((BytesMessage) sourceMsg);
        } else if (sourceMsg instanceof ObjectMessage) {
            msgCopy = duplicateObjectMessage((ObjectMessage) sourceMsg);
        } else if (sourceMsg instanceof MapMessage) {
            msgCopy = duplicateMapMessage((MapMessage) sourceMsg);
        } else if (sourceMsg instanceof StreamMessage) {
            msgCopy = duplicateStreamMessage((StreamMessage) sourceMsg);
        } else {
            return (PulsarMessage) sourceMsg;
        }

        return msgCopy;
    }

    private static PulsarMessage duplicateTextMessage(TextMessage sourceMsg) throws JMSException {
        PulsarTextMessage copy = new PulsarTextMessage();
        copyProperties(sourceMsg, copy);
        copy.setText(sourceMsg.getText());
        return copy;
    }

    private static PulsarMessage duplicateBytesMessage(BytesMessage sourceMsg) throws JMSException {
        PulsarBytesMessage copy = new PulsarBytesMessage();
        copyProperties(sourceMsg, copy);

        sourceMsg.reset();
        int readAmount;
        byte[] buffer = new byte[1024];
        while ((readAmount = sourceMsg.readBytes(buffer)) > 0)
            copy.writeBytes(buffer, 0, readAmount);
        copy.reset();
        return copy;
    }

    private static PulsarMessage duplicateObjectMessage(ObjectMessage sourceMsg) throws JMSException {
        PulsarObjectMessage copy = new PulsarObjectMessage();
        copyProperties(sourceMsg, copy);
        copy.setObject(sourceMsg.getObject());

        return copy;
    }

    private static PulsarMessage duplicateMapMessage(MapMessage sourceMsg) throws JMSException {
        PulsarMapMessage copy = new PulsarMapMessage();
        copyProperties(sourceMsg, copy);

        Enumeration<?> allNames = sourceMsg.getMapNames();
        while (allNames.hasMoreElements()) {
            String name = (String) allNames.nextElement();
            Object value = sourceMsg.getObject(name);
            copy.setObject(name, value);
        }
        return copy;
    }

    private static PulsarMessage duplicateStreamMessage(StreamMessage sourceMsg) throws JMSException {
        PulsarStreamMessage copy = new PulsarStreamMessage();
        copyProperties(sourceMsg, copy);
        sourceMsg.reset();
        Object obj = null;
        try {
            while ((obj = sourceMsg.readObject()) != null) {
                copy.writeObject(obj);
            }
        } catch (MessageEOFException e) {
            // if an end of message stream as expected
        }
        // Writing is finished, so set read only mode true to read messages which sent
        copy.reset();
        return copy;
    }


    public static void copyProperties(Message fromMessage, Message toMessage) throws JMSException {
        toMessage.setJMSMessageID(fromMessage.getJMSMessageID());
        toMessage.setJMSCorrelationID(fromMessage.getJMSCorrelationID());
        //toMessage.setJMSDeliveryMode(fromMessage.getJMSDeliveryMode());
        //toMessage.setJMSRedelivered(fromMessage.getJMSRedelivered());
        toMessage.setJMSReplyTo(fromMessage.getJMSReplyTo());
        toMessage.setJMSDestination(fromMessage.getJMSDestination());
        toMessage.setJMSType(fromMessage.getJMSType());
        toMessage.setJMSExpiration(fromMessage.getJMSExpiration());
        toMessage.setJMSPriority(fromMessage.getJMSPriority());
        toMessage.setJMSTimestamp(fromMessage.getJMSTimestamp());

        Enumeration propertyNames = fromMessage.getPropertyNames();

        while (propertyNames.hasMoreElements()) {
            String name = propertyNames.nextElement().toString();
            Object obj = fromMessage.getObjectProperty(name);
            toMessage.setObjectProperty(name, obj);
        }
    }

    public static boolean isValidType(Object value) {
        if (value == null) {
            throw new NullPointerException("null is not allowed.");
        }
        return object2Type.get(value.getClass()) != null;
    }
}
