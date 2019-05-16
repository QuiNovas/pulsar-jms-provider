package com.echostreams.pulsar.jms.message;

import com.echostreams.pulsar.jms.client.PulsarDestination;
import com.echostreams.pulsar.jms.client.PulsarTopic;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import java.io.Serializable;
import java.util.Enumeration;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.*;

public class PulsarMessageTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageTest.class);

    private String jmsMessageID;
    private String jmsCorrelationID;
    private PulsarDestination jmsDestination;
    private PulsarDestination jmsReplyTo;
    private int jmsDeliveryMode;
    private boolean jmsRedelivered;
    private String jmsType;
    private long jmsExpiration;
    private long jmsTimestamp;
    private long[] consumerIDs;

    @Before
    public void setUp() throws Exception {
        this.jmsMessageID = "ID:TEST-ID:0:0:0:1";
        this.jmsCorrelationID = "testcorrelationId";
        this.jmsDestination = new PulsarTopic("test.topic");
        this.jmsReplyTo = new PulsarTopic("test.replyto.topic:001");
        this.jmsDeliveryMode = Message.DEFAULT_DELIVERY_MODE;
        this.jmsRedelivered = true;
        this.jmsType = "test type";
        this.jmsExpiration = 100000;
        this.jmsTimestamp = System.currentTimeMillis();
        this.consumerIDs = new long[3];
        for (int i = 0; i < this.consumerIDs.length; i++) {
            this.consumerIDs[i] = i;
        }
    }

    @Test
    public void testGetAndSetJMSMessageID() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        assertNull(msg.getJMSMessageID());
        msg.setJMSMessageID(this.jmsMessageID);
        assertEquals(msg.getJMSMessageID(), this.jmsMessageID);
    }

    @Test
    public void testGetAndSetJMSTimestamp() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSTimestamp(this.jmsTimestamp);
        assertTrue(msg.getJMSTimestamp() == this.jmsTimestamp);
    }

    @Test
    public void testGetJMSCorrelationIDAsBytes() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSCorrelationID(this.jmsCorrelationID);
        byte[] testbytes = msg.getJMSCorrelationIDAsBytes();
        String str2 = new String(testbytes);
        assertTrue(this.jmsCorrelationID.equals(str2));
    }

    @Test
    public void testSetJMSCorrelationIDAsBytes() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        byte[] testbytes = this.jmsCorrelationID.getBytes();
        msg.setJMSCorrelationIDAsBytes(testbytes);
        testbytes = msg.getJMSCorrelationIDAsBytes();
        String str2 = new String(testbytes);
        assertTrue(this.jmsCorrelationID.equals(str2));
    }

    @Test
    public void testGetAndSetJMSCorrelationID() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSCorrelationID(this.jmsCorrelationID);
        assertTrue(msg.getJMSCorrelationID().equals(this.jmsCorrelationID));
    }

    @Test
    public void testGetAndSetJMSReplyTo() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSReplyTo(this.jmsReplyTo);
        assertTrue(msg.getJMSReplyTo().equals(this.jmsReplyTo));
    }

    @Test
    public void testGetAndSetJMSDestination() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSDestination(this.jmsDestination);
        assertTrue(msg.getJMSDestination().equals(this.jmsDestination));
    }

    @Test
    public void testGetAndSetJMSDeliveryMode() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSDeliveryMode(this.jmsDeliveryMode);
        assertTrue(msg.getJMSDeliveryMode() == this.jmsDeliveryMode);
        msg.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, msg.getJMSDeliveryMode());
        msg.setJMSDeliveryMode(DeliveryMode.PERSISTENT);
        assertEquals(DeliveryMode.PERSISTENT, msg.getJMSDeliveryMode());
    }

    @Test
    public void testSetJMSDeliveryModeWithNoValue() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();

        try {
            msg.getJMSDeliveryMode();
            fail("Should have thrown an exception");
        } catch (NullPointerException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
    }

    @Test
    public void testGetAndSetMSRedelivered() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSRedelivered(this.jmsRedelivered);
        assertTrue(msg.getJMSRedelivered() == this.jmsRedelivered);
    }

    @Test
    public void testGetAndSetJMSType() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSType(this.jmsType);
        assertTrue(msg.getJMSType().equals(this.jmsType));
    }

    @Test
    public void testGetAndSetJMSExpiration() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setJMSExpiration(this.jmsExpiration);
        assertTrue(msg.getJMSExpiration() == this.jmsExpiration);
    }

    @Test
    public void testGetAndSetJMSPriority() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();

        try {
            msg.getJMSPriority();
            fail("Should have thrown an exception");
        } catch (NullPointerException ex) {
            LOGGER.debug("Exception Occurred: {}", ex.getMessage());
        }
        msg.setJMSPriority(4);

        assertEquals(Message.DEFAULT_PRIORITY, msg.getJMSPriority());
    }

    @Test
    public void testClearProperties() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setStringProperty("test", "test");
        msg.setJMSMessageID(this.jmsMessageID);
        msg.clearProperties();
        assertNull(msg.getStringProperty("test"));
        assertNotNull(msg.getJMSMessageID());
    }

    @Test
    public void testPropertyExists() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setStringProperty("test", "test");
        assertTrue(msg.propertyExists("test"));

        msg.setIntProperty("JMSXDeliveryCount", 1);
        assertTrue(msg.propertyExists("JMSXDeliveryCount"));
    }

    @Test
    public void testGetBooleanProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "booleanProperty";
        msg.setBooleanProperty(name, true);
        assertTrue(msg.getBooleanProperty(name));
    }

    @Test
    public void testGetByteProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "byteProperty";
        msg.setByteProperty(name, (byte) 1);
        assertTrue(msg.getByteProperty(name) == 1);
    }

    @Test
    public void testGetShortProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "shortProperty";
        msg.setShortProperty(name, (short) 1);
        assertTrue(msg.getShortProperty(name) == 1);
    }

    @Test
    public void testGetIntProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "intProperty";
        msg.setIntProperty(name, 1);
        assertTrue(msg.getIntProperty(name) == 1);
    }

    @Test
    public void testGetLongProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "longProperty";
        msg.setLongProperty(name, 1);
        assertTrue(msg.getLongProperty(name) == 1);
    }

    @Test
    public void testGetFloatProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "floatProperty";
        msg.setFloatProperty(name, 1.3f);
        assertTrue(msg.getFloatProperty(name) == 1.3f);
    }

    @Test
    public void testGetDoubleProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "doubleProperty";
        msg.setDoubleProperty(name, 1.3d);
        assertTrue(msg.getDoubleProperty(name) == 1.3);
    }

    @Test
    public void testGetStringProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "stringProperty";
        msg.setStringProperty(name, name);
        assertTrue(msg.getStringProperty(name).equals(name));
    }

    @Test
    public void testGetObjectProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "floatProperty";
        msg.setFloatProperty(name, 1.3f);
        assertTrue(msg.getObjectProperty(name) instanceof Float);
        assertTrue(((Float) msg.getObjectProperty(name)).floatValue() == 1.3f);
    }

    @Test
    public void testGetPropertyNames() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propName = "floatProperty";
        msg.setFloatProperty(propName, 1.3f);
        String jmsxName = "JMSXDeliveryCount";
        msg.setIntProperty(jmsxName, 1);
        String headerName = "JMSRedelivered";
        msg.setBooleanProperty(headerName, false);
        boolean propNameFound = false;
        boolean jmsxNameFound = false;
        boolean headerNameFound1 = false;
        for (Enumeration<?> iter = msg.getPropertyNames(); iter.hasMoreElements(); ) {
            Object element = iter.nextElement();
            propNameFound |= element.equals(propName);
            jmsxNameFound |= element.equals(jmsxName);
        }
        assertTrue("prop name not found", propNameFound);
        assertTrue("jmsx prop name not found", jmsxNameFound);
    }


    @Test
    public void testSetObjectProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "property";

        try {
            msg.setObjectProperty(name, "string");
            msg.setObjectProperty(name, Byte.valueOf("1"));
            msg.setObjectProperty(name, Short.valueOf("1"));
            msg.setObjectProperty(name, Integer.valueOf("1"));
            msg.setObjectProperty(name, Long.valueOf("1"));
            msg.setObjectProperty(name, Float.valueOf("1.1f"));
            msg.setObjectProperty(name, Double.valueOf("1.1"));
            msg.setObjectProperty(name, Boolean.TRUE);
            msg.setObjectProperty(name, null);
        } catch (ClassCastException e) {
            fail("should accept object primitives and String");
        }
    }

    @Test
    public void testConvertProperties() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setStringProperty("stringProperty", "string");
        msg.setByteProperty("byteProperty", Byte.valueOf("1"));
        msg.setShortProperty("shortProperty", Short.valueOf("1"));
        msg.setIntProperty("intProperty", Integer.valueOf("1"));
        msg.setLongProperty("longProperty", Long.valueOf("1"));
        msg.setFloatProperty("floatProperty", Float.valueOf("1.1f"));
        msg.setDoubleProperty("doubleProperty", Double.valueOf("1.1"));
        msg.setBooleanProperty("booleanProperty", Boolean.TRUE);
        msg.setObjectProperty("nullProperty", null);

        assertEquals(msg.getStringProperty("stringProperty"), "string");
        assertEquals(((Byte) msg.getByteProperty("byteProperty")).byteValue(), 1);
        assertEquals(((Short) msg.getShortProperty("shortProperty")).shortValue(), 1);
        assertEquals(((Integer) msg.getIntProperty("intProperty")).intValue(), 1);
        assertEquals(((Long) msg.getLongProperty("longProperty")).longValue(), 1);
        assertEquals(((Float) msg.getFloatProperty("floatProperty")).floatValue(), 1.1f, 0);
        assertEquals(((Double) msg.getDoubleProperty("doubleProperty")).doubleValue(), 1.1, 0);
        assertEquals(((Boolean) msg.getBooleanProperty("booleanProperty")).booleanValue(), true);
    }

    @Test
    public void testSetNullProperty() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String name = "jms";
        msg.setStringProperty(name, "Pulsar");
        assertEquals("Pulsar", msg.getStringProperty(name));

        msg.setStringProperty(name, null);
        assertEquals(null, msg.getStringProperty(name));
    }

    @Test
    public void testSetNullPropertyName() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setStringProperty(null, "Pulsar");
    }

    @Test
    public void testGetAndSetJMSXDeliveryCount() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        msg.setIntProperty("JMSXDeliveryCount", 1);
        int count = msg.getIntProperty("JMSXDeliveryCount");
        assertTrue("expected delivery count = 1 - got: " + count, count == 1);
    }

    @Test
    public void testClearBody() throws JMSException {
        PulsarBytesMessage msg;

        msg = new PulsarBytesMessage();
        msg.clearBody();
        assertFalse(msg.readOnlyBody);
    }

    @Test
    public void testBooleanPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setBooleanProperty(propertyName, true);

        assertEquals(((Boolean) msg.getObjectProperty(propertyName)).booleanValue(), true);
        assertTrue(msg.getBooleanProperty(propertyName));
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testBytePropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setByteProperty(propertyName, (byte) 1);

        assertEquals(((Byte) msg.getObjectProperty(propertyName)).byteValue(), 1);
        assertEquals(msg.getByteProperty(propertyName), 1);

        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testShortPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setShortProperty(propertyName, (short) 1);

        assertEquals(((Short) msg.getObjectProperty(propertyName)).shortValue(), 1);
        assertEquals(msg.getShortProperty(propertyName), 1);
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testIntPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setIntProperty(propertyName, 1);

        assertEquals(((Integer) msg.getObjectProperty(propertyName)).intValue(), 1);
        assertEquals(msg.getIntProperty(propertyName), 1);

        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testLongPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setLongProperty(propertyName, 1);

        assertEquals(((Long) msg.getObjectProperty(propertyName)).longValue(), 1);
        assertEquals(msg.getLongProperty(propertyName), 1);

        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testFloatPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setFloatProperty(propertyName, (float) 1.5);
        assertEquals(((Float) msg.getObjectProperty(propertyName)).floatValue(), 1.5, 0);
        assertEquals(msg.getFloatProperty(propertyName), 1.5, 0);
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testDoublePropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        msg.setDoubleProperty(propertyName, 1.5);
        assertEquals(((Double) msg.getObjectProperty(propertyName)).doubleValue(), 1.5, 0);
        assertEquals(msg.getDoubleProperty(propertyName), 1.5, 0);
        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testStringPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        String stringValue = "true";
        msg.setStringProperty(propertyName, stringValue);
        assertEquals(msg.getStringProperty(propertyName), stringValue);
        assertEquals(msg.getObjectProperty(propertyName), stringValue);

        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        stringValue = "1";
        msg.setStringProperty(propertyName, stringValue);
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        stringValue = "1.5";
        msg.setStringProperty(propertyName, stringValue);
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }

        stringValue = "bad";
        msg.setStringProperty(propertyName, stringValue);
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testObjectPropertyConversion() throws JMSException {
        PulsarMessage msg;

        msg = new PulsarMessage();
        String propertyName = "property";
        Object obj = new Object();
        msg.setObjectProperty(propertyName, obj);

        assertNull(msg.getObjectProperty(null));

        try {
            msg.getStringProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getBooleanProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getByteProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getShortProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getIntProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getLongProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getFloatProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
        try {
            msg.getDoubleProperty(propertyName);
            fail("Should have thrown exception");
        } catch (ClassCastException e) {
        }
    }

    @Test
    public void testMessageIsBodyAssignableTo() throws Exception {
        Message msg;

        msg = new PulsarMessage();
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
    }

    @Test
    public void testTextMessageIsBodyAssignableTo() throws Exception {
        PulsarTextMessage msg;

        msg = new PulsarTextMessage();
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));

        msg.setText("test");

        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testStreamMessageIsBodyAssignableTo() throws Exception {
        PulsarStreamMessage msg;

        msg = new PulsarStreamMessage();
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));

        msg.writeBoolean(false);

        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testMapMessageIsBodyAssignableTo() throws Exception {
        PulsarMapMessage msg;

        msg = new PulsarMapMessage();
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));

        msg.setBoolean("Boolean", true);

        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testBytesMessageIsBodyAssignableTo() throws Exception {
        PulsarBytesMessage msg;

        msg = new PulsarBytesMessage();
        assertFalse(msg.isBodyAssignableTo(byte[].class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));

        msg.writeBoolean(false);

        // The msg doesn't technically have a body until it is reset
        msg.reset();

        assertFalse(msg.isBodyAssignableTo(byte[].class));
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
    }

    @Test
    public void testObjectMessageIsBodyAssignableTo() throws Exception {
        PulsarObjectMessage msg;

        msg = new PulsarObjectMessage();
        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Serializable.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));

        msg.setObject(UUID.randomUUID());

        assertFalse(msg.isBodyAssignableTo(Boolean.class));
        assertFalse(msg.isBodyAssignableTo(Map.class));
        assertTrue(msg.isBodyAssignableTo(String.class));
        assertFalse(msg.isBodyAssignableTo(Serializable.class));
        assertFalse(msg.isBodyAssignableTo(Object.class));
        assertFalse(msg.isBodyAssignableTo(UUID.class));
    }

//--------- Test for getBody method --------------------------------------//

    @Test
    public void testGetBodyOnMessage() throws Exception {
        Message msg;

        msg = new PulsarMessage();
        assertNull(msg.getBody(String.class));

        try {
            msg.getBody(Boolean.class);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }

        try {
            msg.getBody(byte[].class);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }

        try {
            msg.getBody(Object.class);
            fail("Should have thrown exception");
        } catch (MessageFormatException e) {
        }
    }

    @Test
    public void testGetBodyOnTextMessage() throws Exception {
        PulsarTextMessage msg;

        msg = new PulsarTextMessage();
        assertNull(msg.getBody(String.class));
        assertNull(msg.getBody(Boolean.class));
        assertNull(msg.getBody(byte[].class));
        assertNull(msg.getBody(Object.class));

        msg.setText("test");

        assertNotNull(msg.getBody(String.class));
        assertNotNull(msg.getBody(Object.class));
    }

    @Test
    public void testGetBodyOnMapMessage() throws Exception {
        PulsarMapMessage msg;

        msg = new PulsarMapMessage();
        assertNull(msg.getBody(String.class));
        assertNull(msg.getBody(Boolean.class));
        assertNull(msg.getBody(byte[].class));
        assertNull(msg.getBody(Object.class));

        msg.setString("test", "test");

        assertNotNull(msg.getBody(Map.class));
        assertNotNull(msg.getBody(Object.class));
    }

    @Test
    public void testGetBodyOnObjectMessage() throws Exception {
        PulsarObjectMessage msg;

        msg = new PulsarObjectMessage();
        assertNull(msg.getBody(String.class));
        assertNull(msg.getBody(Boolean.class));
        assertNull(msg.getBody(byte[].class));
        assertNull(msg.getBody(Serializable.class));
        assertNull(msg.getBody(Object.class));

        msg.setObject(UUID.randomUUID());

        assertNotNull(msg.getBody(UUID.class));
        assertNotNull(msg.getBody(Serializable.class));
        assertNotNull(msg.getBody(Object.class));
    }

    @Test
    public void testGetBodyOnBytesMessage() throws Exception {
        PulsarBytesMessage msg;

        msg = new PulsarBytesMessage();

        assertNotNull(msg.getBody(String.class));
        assertNotNull(msg.getBody(Boolean.class));
        assertNotNull(msg.getBody(byte[].class));
        assertNotNull(msg.getBody(Object.class));

        msg.clearBody();

        msg.writeUTF("test");
        msg.reset();

        assertNotNull(msg.getBody(byte[].class));
        assertNotNull(msg.getBody(Object.class));
    }

    @Test
    public void testGetBodyOnStreamMessage() throws Exception {
        PulsarStreamMessage msg;

        msg = new PulsarStreamMessage();

        assertNotNull(msg.getBody(Object.class));
        assertNotNull(msg.getBody(Boolean.class));
        assertNotNull(msg.getBody(String.class));
        assertNotNull(msg.getBody(byte[].class));
    }

    @Test
    public void testToString() throws Exception {
        PulsarMessage msg;

        msg = new PulsarMessage();
        assertTrue(msg.toString().startsWith("PulsarMessage"));
    }
}
