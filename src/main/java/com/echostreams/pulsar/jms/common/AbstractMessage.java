package com.echostreams.pulsar.jms.common;

import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import java.lang.ref.WeakReference;
import java.util.Enumeration;
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
    private Map<String,Object> propertyMap;

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
        return id != null ? "ID:"+id : null;
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
        return null;
    }

    @Override
    public void setJMSReplyTo(Destination destination) throws JMSException {

    }

    @Override
    public Destination getJMSDestination() throws JMSException {
        return null;
    }

    @Override
    public void setJMSDestination(Destination destination) throws JMSException {

    }

    @Override
    public int getJMSDeliveryMode() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSDeliveryMode(int i) throws JMSException {

    }

    @Override
    public boolean getJMSRedelivered() throws JMSException {
        return false;
    }

    @Override
    public void setJMSRedelivered(boolean b) throws JMSException {

    }

    @Override
    public String getJMSType() throws JMSException {
        return null;
    }

    @Override
    public void setJMSType(String s) throws JMSException {

    }

    @Override
    public long getJMSExpiration() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSExpiration(long l) throws JMSException {

    }

    @Override
    public int getJMSPriority() throws JMSException {
        return 0;
    }

    @Override
    public void setJMSPriority(int i) throws JMSException {

    }

    @Override
    public void clearProperties() throws JMSException {

    }

    @Override
    public boolean propertyExists(String s) throws JMSException {
        return false;
    }

    @Override
    public boolean getBooleanProperty(String s) throws JMSException {
        return false;
    }

    @Override
    public byte getByteProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public short getShortProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public int getIntProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public long getLongProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public float getFloatProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public double getDoubleProperty(String s) throws JMSException {
        return 0;
    }

    @Override
    public String getStringProperty(String s) throws JMSException {
        return null;
    }

    @Override
    public Object getObjectProperty(String s) throws JMSException {
        return null;
    }

    @Override
    public Enumeration getPropertyNames() throws JMSException {
        return null;
    }

    @Override
    public void setBooleanProperty(String s, boolean b) throws JMSException {

    }

    @Override
    public void setByteProperty(String s, byte b) throws JMSException {

    }

    @Override
    public void setShortProperty(String s, short i) throws JMSException {

    }

    @Override
    public void setIntProperty(String s, int i) throws JMSException {

    }

    @Override
    public void setLongProperty(String s, long l) throws JMSException {

    }

    @Override
    public void setFloatProperty(String s, float v) throws JMSException {

    }

    @Override
    public void setDoubleProperty(String s, double v) throws JMSException {

    }

    @Override
    public void setStringProperty(String s, String s1) throws JMSException {

    }

    @Override
    public void setObjectProperty(String s, Object o) throws JMSException {

    }

    protected final AbstractSession getSession() throws JMSException
    {
        if (sessionRef == null)
            throw new PulsarJMSException("Message has no associated session","CONSISTENCY");

        AbstractSession session = sessionRef.get();
        if (session == null)
            throw new PulsarJMSException("Message session is no longer valid","CONSISTENCY");

        return session;
    }

    @Override
    public void acknowledge() throws JMSException {
        AbstractSession session = getSession();

        int acknowledgeMode = session.getAcknowledgeMode();
        if (acknowledgeMode != Session.CLIENT_ACKNOWLEDGE)
            return; // Ignore [JMS SPEC]

        //session.acknowledge();
    }

    @Override
    public void clearBody() throws JMSException {

    }

    protected final synchronized void assertDeserializationLevel( int targetLevel )
    {
        if (rawMessage == null)
            return; // Not a serialized message or fully deserialized message

        if (unserializationLevel < targetLevel)
            throw new IllegalStateException("Message is not deserialized (level="+unserializationLevel+")");
    }
}
