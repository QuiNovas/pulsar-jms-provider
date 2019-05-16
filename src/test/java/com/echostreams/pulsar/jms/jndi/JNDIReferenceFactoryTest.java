package com.echostreams.pulsar.jms.jndi;

import com.echostreams.pulsar.jms.client.PulsarConnectionFactory;
import com.echostreams.pulsar.jms.client.PulsarQueue;
import com.echostreams.pulsar.jms.client.PulsarTopic;
import org.junit.Before;
import org.junit.Test;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.naming.*;
import javax.naming.spi.ObjectFactory;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.NoSuchElementException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class JNDIReferenceFactoryTest {

    private static final String DESTINATION_NAME_PROP = "address";
    private static final String REMOTE_URI_NAME_PROP = "remoteURI";

    private static final String TEST_CONNECTION_URL = "pulsar://example.com:6650";
    private static final String TEST_QUEUE_ADDRESS = "myQueue";
    private static final String TEST_TOPIC_ADDRESS = "myTopic";

    private Name mockName;
    private Context mockContext;
    private Hashtable<?, ?> testEnvironment;
    private ObjectFactory referenceFactory;

    @Before
    public void setUp() throws Exception {
        mockName = mock(Name.class);
        mockContext = mock(Context.class);
        testEnvironment = new Hashtable<>();

        referenceFactory = new JNDIReferenceFactory();
    }

    @Test
    public void testGetObjectInstanceCreatesPulsarConnectionFactory() throws Exception {
        Reference reference = createTestReference(PulsarConnectionFactory.class.getName(), REMOTE_URI_NAME_PROP, TEST_CONNECTION_URL);

        Object connFactory = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);

        assertNotNull("Expected object to be created", connFactory);
        assertEquals("Unexpected object type created", PulsarConnectionFactory.class, connFactory.getClass());
    }

    @Test
    public void testGetObjectInstanceCreatesPulsarQueue() throws Exception {
        Reference reference = createTestReference(PulsarQueue.class.getName(), DESTINATION_NAME_PROP, TEST_QUEUE_ADDRESS);

        Object queue = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment); // returns null as Queue doesn't use jndi

        assertNull("Expected object to be created", queue);

        try {
            queue.getClass();
            fail("Should throw exception");
        } catch (NullPointerException npe) {
        }

        try {
            ((Queue) queue).getQueueName();
            fail("Should throw exception");
        } catch (NullPointerException npe) {
        }
    }

    @Test
    public void testGetObjectInstanceCreatesPulsarTopic() throws Exception {
        Reference reference = createTestReference(PulsarTopic.class.getName(), DESTINATION_NAME_PROP, TEST_TOPIC_ADDRESS);

        Object topic = referenceFactory.getObjectInstance(reference, mockName, mockContext, testEnvironment);// returns null as Topic doesn't use jndi

        assertNull("Expected object to be created", topic);

        try {
            topic.getClass();
            fail("Should throw exception");
        } catch (NullPointerException npe) {
        }

        try {
            ((Topic) topic).getTopicName();
            fail("Should throw exception");
        } catch (NullPointerException npe) {
        }
    }

    private Reference createTestReference(String className, String addressType, Object content) {
        Reference mockReference = mock(Reference.class);
        when(mockReference.getClassName()).thenReturn(className);

        RefAddr mockRefAddr = mock(StringRefAddr.class);
        when(mockRefAddr.getType()).thenReturn(addressType);
        when(mockRefAddr.getContent()).thenReturn(content);

        RefAddrTestEnumeration testEnumeration = new RefAddrTestEnumeration(mockRefAddr);
        when(mockReference.getAll()).thenReturn(testEnumeration);

        return mockReference;
    }

    private class RefAddrTestEnumeration implements Enumeration<RefAddr> {
        boolean hasMore = true;
        final RefAddr element;

        public RefAddrTestEnumeration(RefAddr mockAddr) {
            element = mockAddr;
        }

        @Override
        public boolean hasMoreElements() {
            return hasMore;
        }

        @Override
        public RefAddr nextElement() {
            if (!hasMore) {
                throw new NoSuchElementException("No more elements");
            }

            hasMore = false;
            return element;
        }
    }

}
