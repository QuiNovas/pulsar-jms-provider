package com.echostreams.pulsar.jms.client;

import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Test;
import org.mockito.Mockito;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;

public class PulsarJMSProducerTest extends PulsarConnectionTestSupport {

    private final String STRING_PROPERTY_NAME = "StringProperty";
    private final String STRING_PROPERTY_VALUE = UUID.randomUUID().toString();

    private final String BYTE_PROPERTY_NAME = "ByteProperty";
    private final byte BYTE_PROPERTY_VALUE = Byte.MAX_VALUE;

    private final String BOOLEAN_PROPERTY_NAME = "BooleanProperty";
    private final boolean BOOLEAN_PROPERTY_VALUE = Boolean.TRUE;

    private final String SHORT_PROPERTY_NAME = "ShortProperty";
    private final short SHORT_PROPERTY_VALUE = Short.MAX_VALUE;

    private final String INTEGER_PROPERTY_NAME = "IntegerProperty";
    private final int INTEGER_PROPERTY_VALUE = Integer.MAX_VALUE;

    private final String LONG_PROPERTY_NAME = "LongProperty";
    private final long LONG_PROPERTY_VALUE = Long.MAX_VALUE;

    private final String DOUBLE_PROPERTY_NAME = "DoubleProperty";
    private final double DOUBLE_PROPERTY_VALUE = Double.MAX_VALUE;

    private final String FLOAT_PROPERTY_NAME = "FloatProperty";
    private final float FLOAT_PROPERTY_VALUE = Float.MAX_VALUE;

    private final Destination JMS_DESTINATION = new PulsarTopic("test.target.topic:001");
    private final Destination JMS_REPLY_TO = new PulsarTopic("test.replyto.topic:001");
    private final String JMS_CORRELATION_ID = UUID.randomUUID().toString();
    private final String JMS_TYPE_STRING = "TestType";

    @Test
    public void testGetPropertyNames() throws JMSException, PulsarClientException {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.getPropertyNames().contains("Property_1"));
        assertTrue(producer.getPropertyNames().contains("Property_2"));
        assertTrue(producer.getPropertyNames().contains("Property_3"));
    }

    @Test
    public void testClearProperties() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        producer.clearProperties();

        assertEquals(0, producer.getPropertyNames().size());
    }

    @Test
    public void testPropertyExists() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty("Property_1", "1");
        producer.setProperty("Property_2", "2");
        producer.setProperty("Property_3", "3");

        assertEquals(3, producer.getPropertyNames().size());

        assertTrue(producer.propertyExists("Property_1"));
        assertTrue(producer.propertyExists("Property_2"));
        assertTrue(producer.propertyExists("Property_3"));
        assertFalse(producer.propertyExists("Property_4"));
    }

    @Test
    public void testJMSCorrelationID() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        assertEquals(JMS_CORRELATION_ID, producer.getJMSCorrelationID());
    }

    @Test
    public void testJMSCorrelationIDBytes() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes(StandardCharsets.UTF_8));
        assertEquals(JMS_CORRELATION_ID, new String(producer.getJMSCorrelationIDAsBytes(), StandardCharsets.UTF_8));
    }

    @Test
    public void testJMSReplyTo() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setJMSReplyTo(JMS_REPLY_TO);
        assertFalse(JMS_REPLY_TO.equals(producer.getJMSReplyTo()));
    }

    @Test
    public void testJMSType() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setJMSType(JMS_TYPE_STRING);
        assertEquals(JMS_TYPE_STRING, producer.getJMSType());
    }

    @Test
    public void testGetStringProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        assertEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
    }

    @Test
    public void testGetByteProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
    }

    @Test
    public void testGetBooleanProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
    }

    @Test
    public void testGetShortProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
    }

    @Test
    public void testGetIntegerProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
    }

    @Test
    public void testGetLongProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
    }

    @Test
    public void testGetDoubleProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
    }

    @Test
    public void testGetFloatProperty() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);
    }

    @Test
    public void testSetPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, Byte.valueOf(BYTE_PROPERTY_VALUE));
        producer.setProperty(BOOLEAN_PROPERTY_NAME, Boolean.valueOf(BOOLEAN_PROPERTY_VALUE));
        producer.setProperty(SHORT_PROPERTY_NAME, Short.valueOf(SHORT_PROPERTY_VALUE));
        producer.setProperty(INTEGER_PROPERTY_NAME, Integer.valueOf(INTEGER_PROPERTY_VALUE));
        producer.setProperty(LONG_PROPERTY_NAME, Long.valueOf(LONG_PROPERTY_VALUE));
        producer.setProperty(FLOAT_PROPERTY_NAME, Float.valueOf(FLOAT_PROPERTY_VALUE));
        producer.setProperty(DOUBLE_PROPERTY_NAME, Double.valueOf(DOUBLE_PROPERTY_VALUE));
        producer.setProperty(STRING_PROPERTY_NAME, UUID.randomUUID());


        assertNull(producer.getObjectProperty("Unknown"));

        assertNotEquals(STRING_PROPERTY_VALUE, producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0);
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);

        assertNotEquals(STRING_PROPERTY_VALUE, producer.getObjectProperty(STRING_PROPERTY_NAME));
        assertEquals(BYTE_PROPERTY_VALUE, producer.getObjectProperty(BYTE_PROPERTY_NAME));
        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getObjectProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getObjectProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getObjectProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getObjectProperty(LONG_PROPERTY_NAME));
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getObjectProperty(FLOAT_PROPERTY_NAME));
        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getObjectProperty(DOUBLE_PROPERTY_NAME));
    }

    @Test
    public void testStringPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(String.valueOf(STRING_PROPERTY_VALUE), producer.getStringProperty(STRING_PROPERTY_NAME));
        assertEquals(String.valueOf(BYTE_PROPERTY_VALUE), producer.getStringProperty(BYTE_PROPERTY_NAME));
        assertEquals(String.valueOf(BOOLEAN_PROPERTY_VALUE), producer.getStringProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(String.valueOf(SHORT_PROPERTY_VALUE), producer.getStringProperty(SHORT_PROPERTY_NAME));
        assertEquals(String.valueOf(INTEGER_PROPERTY_VALUE), producer.getStringProperty(INTEGER_PROPERTY_NAME));
        assertEquals(String.valueOf(LONG_PROPERTY_VALUE), producer.getStringProperty(LONG_PROPERTY_NAME));
        assertEquals(String.valueOf(FLOAT_PROPERTY_VALUE), producer.getStringProperty(FLOAT_PROPERTY_NAME));
        assertEquals(String.valueOf(DOUBLE_PROPERTY_VALUE), producer.getStringProperty(DOUBLE_PROPERTY_NAME));
    }

    @Test
    public void testBytePropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getByteProperty(BYTE_PROPERTY_NAME));

        try {
            producer.getByteProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getByteProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getByteProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testBooleanPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BOOLEAN_PROPERTY_VALUE, producer.getBooleanProperty(BOOLEAN_PROPERTY_NAME));
        assertEquals(Boolean.FALSE, producer.getBooleanProperty(STRING_PROPERTY_NAME));

        try {
            producer.getBooleanProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getBooleanProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testShortPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getShortProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getShortProperty(SHORT_PROPERTY_NAME));

        try {
            producer.getShortProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getShortProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getShortProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testIntPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getIntProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getIntProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getIntProperty(INTEGER_PROPERTY_NAME));

        try {
            producer.getIntProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getIntProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getIntProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testLongPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(BYTE_PROPERTY_VALUE, producer.getLongProperty(BYTE_PROPERTY_NAME));
        assertEquals(SHORT_PROPERTY_VALUE, producer.getLongProperty(SHORT_PROPERTY_NAME));
        assertEquals(INTEGER_PROPERTY_VALUE, producer.getLongProperty(INTEGER_PROPERTY_NAME));
        assertEquals(LONG_PROPERTY_VALUE, producer.getLongProperty(LONG_PROPERTY_NAME));

        try {
            producer.getLongProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getLongProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(FLOAT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getLongProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testFloatPropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(FLOAT_PROPERTY_VALUE, producer.getFloatProperty(FLOAT_PROPERTY_NAME), 0.0f);

        try {
            producer.getFloatProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getFloatProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getFloatProperty(DOUBLE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testDoublePropertyConversions() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setProperty(STRING_PROPERTY_NAME, STRING_PROPERTY_VALUE);
        producer.setProperty(BYTE_PROPERTY_NAME, BYTE_PROPERTY_VALUE);
        producer.setProperty(BOOLEAN_PROPERTY_NAME, BOOLEAN_PROPERTY_VALUE);
        producer.setProperty(SHORT_PROPERTY_NAME, SHORT_PROPERTY_VALUE);
        producer.setProperty(INTEGER_PROPERTY_NAME, INTEGER_PROPERTY_VALUE);
        producer.setProperty(LONG_PROPERTY_NAME, LONG_PROPERTY_VALUE);
        producer.setProperty(FLOAT_PROPERTY_NAME, FLOAT_PROPERTY_VALUE);
        producer.setProperty(DOUBLE_PROPERTY_NAME, DOUBLE_PROPERTY_VALUE);

        assertEquals(DOUBLE_PROPERTY_VALUE, producer.getDoubleProperty(DOUBLE_PROPERTY_NAME), 0.0);
        assertEquals(FLOAT_PROPERTY_VALUE, producer.getDoubleProperty(FLOAT_PROPERTY_NAME), 0.0);

        try {
            producer.getDoubleProperty(STRING_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (NumberFormatException nfe) {
        }
        try {
            producer.getDoubleProperty(BOOLEAN_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(SHORT_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(INTEGER_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(LONG_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
        try {
            producer.getDoubleProperty(BYTE_PROPERTY_NAME);
            fail("Should not be able to convert");
        } catch (MessageFormatRuntimeException mfre) {
        }
    }

    @Test
    public void testSetPropertyWithNull() {
        PulsarJMSProducer producer = mock(PulsarJMSProducer.class);

        assertNull(producer.setProperty(null, 100));
        assertNull(producer.setProperty(null, 100l));
        assertNull(producer.setProperty(null, UUID.randomUUID()));
        assertNull(producer.setProperty(null, (short) 100));
        assertNull(producer.setProperty(null, 100.0f));
        assertNull(producer.setProperty(null, 100.0));
        assertNull(producer.setProperty(null, true));
        assertNull(producer.setProperty(null, (byte) 1));
        assertNull(producer.setProperty(null, "test"));


    }

    @Test
    public void testAsync() {
        PulsarJMSProducer producer = new PulsarJMSProducer();
        TestPulsarJmsCompletionListener listener = new TestPulsarJmsCompletionListener();

        producer.setAsync(listener);
        assertEquals(listener, producer.getAsync());
    }

    @Test
    public void testDeliveryMode() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
        assertEquals(DeliveryMode.NON_PERSISTENT, producer.getDeliveryMode());
    }

    @Test(timeout = 10000)
    public void testDeliveryModeConfigurationWithInvalidMode() throws Exception {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());

        try {
            producer.setDeliveryMode(-1);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        try {
            producer.setDeliveryMode(5);
            fail("Should have thrown an exception");
        } catch (JMSRuntimeException ex) {
            // Expected
        }

        assertEquals(Message.DEFAULT_DELIVERY_MODE, producer.getDeliveryMode());
    }

    @Test
    public void testDeliveryDelay() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setDeliveryDelay(2000);
        assertEquals(2000, producer.getDeliveryDelay());
    }

    @Test
    public void testDisableMessageID() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setDisableMessageID(false);
        assertEquals(false, producer.getDisableMessageID());
        producer.setDisableMessageID(true);
        assertEquals(true, producer.getDisableMessageID());
    }

    @Test
    public void testMessageDisableTimestamp() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setDisableMessageTimestamp(false);
        assertEquals(false, producer.getDisableMessageTimestamp());
        producer.setDisableMessageTimestamp(true);
        assertEquals(true, producer.getDisableMessageTimestamp());
    }

    @Test
    public void testPriority() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setPriority(1);
        assertEquals(1, producer.getPriority());
        producer.setPriority(4);
        assertEquals(4, producer.getPriority());
    }

    @Test
    public void testTimeToLive() {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        producer.setTimeToLive(2000);
        assertEquals(2000, producer.getTimeToLive());
    }

    @Test
    public void testSendNullMessageReturnNull() throws JMSException {
        PulsarJMSProducer producer = mock(PulsarJMSProducer.class);

        assertNull(producer.send(JMS_DESTINATION, (Message) null));
    }

    @Test
    public void testSendJMSMessage() throws JMSException {
        PulsarJMSProducer producer = new PulsarJMSProducer();

        Message message = mock(Message.class);

        producer.setJMSCorrelationID(JMS_CORRELATION_ID);
        producer.setJMSCorrelationIDAsBytes(JMS_CORRELATION_ID.getBytes());
        producer.setJMSReplyTo(JMS_REPLY_TO);
        producer.setJMSType(JMS_TYPE_STRING);

        try {
            producer.send(JMS_DESTINATION, message);
            fail("Should not be able to convert");
        } catch (ClassCastException cce) {
        }

        Mockito.verify(message).setJMSCorrelationID(JMS_CORRELATION_ID);
        Mockito.verify(message).setJMSType(JMS_TYPE_STRING);
    }

    @Test
    public void testRuntimeExceptionFromSendMessage() throws JMSException {
        PulsarSession session = mock(PulsarSession.class);
        Message message = mock(Message.class);

        Mockito.when(session.createTopic("test")).thenReturn(new PulsarTopic("test"));
        Mockito.when(session.createMessage()).thenReturn(message);

        Mockito.doThrow(IllegalStateException.class).when(message).setJMSCorrelationID(anyString());

        PulsarJMSProducer producer = mock(PulsarJMSProducer.class);

        producer.setJMSCorrelationID("id");

        assertNull(producer.send(session.createTopic("test"), session.createMessage()));

    }

    private class TestPulsarJmsCompletionListener implements CompletionListener {

        @Override
        public void onCompletion(Message message) {
        }

        @Override
        public void onException(Message message, Exception exception) {
        }
    }
}
