package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.common.destination.DestinationSerializer;
import com.echostreams.pulsar.jms.common.destination.DestinationTools;
import com.echostreams.pulsar.jms.utils.EmptyEnumeration;
import com.echostreams.pulsar.jms.utils.IteratorEnumeration;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.RawDataBuffer;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.lang.ref.WeakReference;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public abstract class AbstractMessage implements Message {

    // Persistent properties
    private String id;
    private String correlId;
    private int priority;
    private int deliveryMode;
    private Destination destination;
    private long expiration;
    private boolean redelivered;
    private Destination replyTo;
    private long timestamp;
    private String type;
    private Map<String, Object> propertyMap;

    // Serialization related
    private int unserializationLevel;
    private RawDataBuffer rawMessage;

    // Volatile properties
    private boolean propertiesAreReadOnly;
    protected boolean bodyIsReadOnly;
    private transient WeakReference<AbstractSession> sessionRef; // Weak link to the parent session
    private transient boolean internalCopy = false;

    public AbstractMessage() {
        super();
    }


    @Override
    public String getJMSMessageID() throws JMSException {
        return id != null ? "ID:" + id : null;
    }

    @Override
    public void setJMSMessageID(String id) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.id = id;
    }

    @Override
    public long getJMSTimestamp() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return timestamp;
    }

    @Override
    public void setJMSTimestamp(long timestamp) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.timestamp = timestamp;
    }

    @Override
    public byte[] getJMSCorrelationIDAsBytes() throws JMSException {
        return new byte[0];
    }

    @Override
    public void setJMSCorrelationIDAsBytes(byte[] bytes) throws JMSException {

    }

    @Override
    public void setJMSCorrelationID(String correlationID) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.correlId = correlationID;
    }

    @Override
    public String getJMSCorrelationID() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return correlId;
    }

    @Override
    public Destination getJMSReplyTo() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return replyTo;
    }

    @Override
    public void setJMSReplyTo(Destination replyTo) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.replyTo = replyTo;
    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return destination;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.destination = DestinationTools.asRef(destination);
    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return deliveryMode;
    }

    @Override
    public void setJMSDeliveryMode(int deliveryMode) throws JMSException {
        if (deliveryMode != DeliveryMode.PERSISTENT &&
                deliveryMode != DeliveryMode.NON_PERSISTENT)
            throw new PulsarJMSException("Invalid delivery mode : " + deliveryMode, "INVALID_DELIVERY_MODE");

        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.deliveryMode = deliveryMode;
    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return redelivered;
    }

    @Override
    public void setJMSRedelivered(boolean redelivered) throws JMSException {
        this.redelivered = redelivered;

        // Update raw cache accordingly
        if (rawMessage != null) {
            byte flags = rawMessage.readByte(1);
            if (redelivered)
                flags = (byte) (flags | (1 << 4));
            else
                flags = (byte) (flags & ~(1 << 4));

            rawMessage.writeByte(flags, 1);
        }
    }

    @Override
    public String getJMSType() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return type;
    }

    @Override
    public void setJMSType(String type) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.type = type;
    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return expiration;
    }

    @Override
    public void setJMSExpiration(long expiration) throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.expiration = expiration;
    }

    @Override
    public int getJMSPriority() throws JMSException {
        return priority;
    }

    @Override
    public void setJMSPriority(int priority) throws JMSException {
        if (priority < 0 || priority > 9)
            throw new PulsarJMSException("Invalid priority value : " + priority, "INVALID_PRIORITY");

        assertDeserializationLevel(MessageSerializationLevel.FULL);
        this.priority = priority;
    }

    @Override
    public void clearProperties() throws JMSException {
        if (propertyMap != null) propertyMap.clear();
        propertiesAreReadOnly = false;
    }

    @Override
    public boolean propertyExists(String name) throws JMSException {
        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Empty property name");

        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return propertyMap != null ? propertyMap.containsKey(name) : false;
    }

    @Override
    public boolean getBooleanProperty(String name) throws JMSException {
        return MessageConverter.asBoolean(getProperty(name));
    }

    private Object getProperty(String name) {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);
        return propertyMap != null ? propertyMap.get(name) : null;
    }

    @Override
    public byte getByteProperty(String name) throws JMSException {
        return MessageConverter.asByte(getProperty(name));
    }

    @Override
    public short getShortProperty(String name) throws JMSException {
        return MessageConverter.asShort(getProperty(name));
    }

    @Override
    public int getIntProperty(String name) throws JMSException {
        return MessageConverter.asInt(getProperty(name));
    }

    @Override
    public long getLongProperty(String name) throws JMSException {
        return MessageConverter.asLong(getProperty(name));
    }

    @Override
    public float getFloatProperty(String name) throws JMSException {
        return MessageConverter.asFloat(getProperty(name));
    }

    @Override
    public double getDoubleProperty(String name) throws JMSException {
        return MessageConverter.asDouble(getProperty(name));
    }

    @Override
    public String getStringProperty(String name) throws JMSException {
        return MessageConverter.asString(getProperty(name));
    }

    @Override
    public Object getObjectProperty(String name) throws JMSException {
        return getProperty(name);
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        assertDeserializationLevel(MessageSerializationLevel.ALL_HEADERS);

        if (propertyMap == null)
            return new EmptyEnumeration<>();

        return new IteratorEnumeration<>(propertyMap.keySet().iterator());
    }

    @Override
    public void setBooleanProperty(String name, boolean value) throws JMSException {
        setProperty(name, Boolean.valueOf(value));
    }

    @Override
    public void setByteProperty(String name, byte value) throws JMSException {
        setProperty(name, Byte.valueOf(value));
    }

    @Override
    public void setShortProperty(String name, short value) throws JMSException {
        setProperty(name, Short.valueOf(value));
    }

    @Override
    public void setIntProperty(String name, int value) throws JMSException {
        setProperty(name, Integer.valueOf(value));
    }

    @Override
    public void setLongProperty(String name, long value) throws JMSException {
        setProperty(name, Long.valueOf(value));
    }

    @Override
    public void setFloatProperty(String name, float value) throws JMSException {
        setProperty(name, new Float(value));
    }

    @Override
    public void setDoubleProperty(String name, double value) throws JMSException {
        setProperty(name, new Double(value));
    }

    @Override
    public void setStringProperty(String name, String value) throws JMSException {
        setProperty(name, value);
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        if (value == null)
            throw new MessageFormatException("A property value cannot be null");

        // Check type
        if (!(value instanceof Boolean ||
                value instanceof Byte ||
                value instanceof Short ||
                value instanceof Integer ||
                value instanceof Long ||
                value instanceof Float ||
                value instanceof Double ||
                value instanceof String))
            throw new MessageFormatException("Unsupported property value type : " + value.getClass().getName());

        setProperty(name, value);
    }

    private void setProperty(String name, Object value) throws JMSException {
        if (propertiesAreReadOnly)
            throw new MessageNotWriteableException("Message properties are read-only");

        if (name == null || name.length() == 0)
            throw new IllegalArgumentException("Empty property name");

        assertDeserializationLevel(MessageSerializationLevel.FULL);

        if (propertyMap == null)
            propertyMap = new HashMap<>(17);
        propertyMap.put(name, value);
    }

    protected final AbstractSession getSession() throws JMSException {
        if (sessionRef == null)
            throw new PulsarJMSException("Message has no associated session", "CONSISTENCY");

        AbstractSession session = sessionRef.get();
        if (session == null)
            throw new PulsarJMSException("Message session is no longer valid", "CONSISTENCY");

        return session;
    }

    @Override
    public void acknowledge() throws JMSException {
        AbstractSession session = getSession();

        int acknowledgeMode = session.getAcknowledgeMode();
        if (acknowledgeMode != Session.CLIENT_ACKNOWLEDGE)
            return; // Ignore [JMS SPEC]

        session.acknowledge();
    }

    protected final synchronized void assertDeserializationLevel(int targetLevel) {
        if (rawMessage == null)
            return; // Not a serialized message or fully deserialized message

        if (unserializationLevel < targetLevel)
            throw new IllegalStateException("Message is not deserialized (level=" + unserializationLevel + ")");
    }

    public final void setSession(AbstractSession session) throws JMSException {
        if (session == null)
            this.sessionRef = null;
        else {
            // Consistency check
            if (sessionRef != null && sessionRef.get() != session)
                throw new PulsarJMSException("Message session already set", "CONSISTENCY");

            this.sessionRef = new WeakReference<>(session);
        }
    }

    protected abstract byte getType();

    public abstract AbstractMessage copy();

    protected final void copyCommonFields(AbstractMessage clone) {
        clone.id = this.id;
        clone.correlId = this.correlId;
        clone.priority = this.priority;
        clone.deliveryMode = this.deliveryMode;
        clone.destination = this.destination;
        clone.expiration = this.expiration;
        clone.redelivered = this.redelivered;
        clone.replyTo = this.replyTo;
        clone.timestamp = this.timestamp;
        clone.type = this.type;

        @SuppressWarnings("unchecked")
        Map<String, Object> propertyMapClone = this.propertyMap != null ? (Map<String, Object>) ((HashMap<String, Object>) this.propertyMap).clone() : null;
        clone.propertyMap = propertyMapClone;

        // Copy raw message cache if any
        clone.unserializationLevel = this.unserializationLevel;
        if (this.rawMessage != null)
            clone.rawMessage = this.rawMessage.copy();
    }

    protected abstract void serializeBodyTo(RawDataBuffer out);

    protected abstract void unserializeBodyFrom(RawDataBuffer in);

    public final RawDataBuffer getRawMessage()
    {
        return rawMessage;
    }

    public final synchronized void ensureDeserializationLevel( int targetLevel )
    {
        if (rawMessage == null)
            return; // Not a serialized message or fully deserialized message

        while (unserializationLevel < targetLevel)
        {
            if (unserializationLevel == MessageSerializationLevel.BASE_HEADERS)
            {
                // Read level 2 headers
                byte lvl2Flags = rawMessage.readByte();
                if ((lvl2Flags & (1 << 0)) != 0) correlId = rawMessage.readUTF();
                if ((lvl2Flags & (1 << 1)) != 0) replyTo = DestinationSerializer.unserializeFrom(rawMessage);
                if ((lvl2Flags & (1 << 2)) != 0) timestamp = rawMessage.readLong();
                if ((lvl2Flags & (1 << 3)) != 0) type = rawMessage.readUTF();
                if ((lvl2Flags & (1 << 4)) != 0) propertyMap = readMapFrom(rawMessage);

                unserializationLevel = MessageSerializationLevel.ALL_HEADERS;
            }
            else
            if (unserializationLevel == MessageSerializationLevel.ALL_HEADERS)
            {
                // Read level 3 - message body
                unserializeBodyFrom(rawMessage);
                unserializationLevel = MessageSerializationLevel.FULL;
                rawMessage = null; // Save memory
            }
        }
    }

    /**
     * Write a map to the given output stream
     */
    protected final Map<String,Object> readMapFrom( RawDataBuffer in )
    {
        int mapSize = in.readInt();
        if (mapSize == 0)
            return null;

        Map<String,Object> map = new HashMap<>(Math.max(17,mapSize*4/3));
        for (int n = 0 ; n < mapSize ; n++)
        {
            String propName = in.readUTF();
            Object propValue = in.readGeneric();
            map.put(propName,propValue);
        }

        return map;
    }

    public final boolean isInternalCopy()
    {
        return internalCopy;
    }

    public final void setInternalCopy(boolean copy)
    {
        this.internalCopy = copy;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("[messageId=");
        sb.append(id);
        sb.append(" priority=");
        sb.append(priority);
        sb.append(" correlId=");
        sb.append(correlId);
        sb.append(" deliveryMode=");
        sb.append(deliveryMode);
        sb.append(" destination=");
        sb.append(destination);
        sb.append(" expiration=");
        sb.append(expiration);
        sb.append(" redelivered=");
        sb.append(redelivered);
        sb.append(" replyTo=");
        sb.append(replyTo);
        sb.append(" timestamp=");
        sb.append(timestamp);
        sb.append(" type=");
        sb.append(type);

        if (propertyMap != null && propertyMap.size() > 0)
        {
            sb.append(" properties=");
            Iterator<String> allProps = propertyMap.keySet().iterator();
            int count = 0;
            while (allProps.hasNext())
            {
                String propName = allProps.next();
                Object propValue = propertyMap.get(propName);
                if (count++ > 0)
                    sb.append(",");
                sb.append(propName);
                sb.append("=");
                sb.append(propValue);
            }
        }
        sb.append("]");

        return sb.toString();
    }
}
