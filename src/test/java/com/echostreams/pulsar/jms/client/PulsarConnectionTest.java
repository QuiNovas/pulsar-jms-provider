package com.echostreams.pulsar.jms.client;

import org.junit.Before;
import org.junit.Test;

import javax.jms.*;

import java.io.IOException;

import static org.junit.Assert.*;

public class PulsarConnectionTest extends PulsarConnectionTestSupport {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        connection = createConnectionToMockPulsarClient();
    }

    @Test(timeout = 30000)
    public void testCreateSessionDefaultMode() throws JMSException, IOException {
        PulsarSession session = (PulsarSession) connection.createSession();
        assertEquals(session.getAcknowledgeMode(), Session.AUTO_ACKNOWLEDGE);
        connection.close();
    }

    @Test(timeout = 30000)
    public void testConnectionMetaData() throws Exception {
        ConnectionMetaData metaData = connection.getMetaData();

        assertNotNull(metaData);
        assertEquals(2, metaData.getJMSMajorVersion());
        assertEquals(1, metaData.getJMSMinorVersion());
        assertEquals("2.1", metaData.getJMSVersion());
        assertNotNull(metaData.getJMSXPropertyNames());

        assertNotNull(metaData.getProviderVersion());
        assertNotNull(metaData.getJMSProviderName());

        int major = metaData.getProviderMajorVersion();
        int minor = metaData.getProviderMinorVersion();
        assertTrue("Expected non-zero provider major(" + major + ") / minor(" + minor + ") version.", (major + minor) >= 0);
    }

    @Test(timeout = 30000)
    public void testCreateConnectionConsumerOnQueueConnection() throws JMSException {
        QueueConnection queueConnection = createQueueConnectionToMockPulsarClient();
        queueConnection.createQueueSession(true, Session.AUTO_ACKNOWLEDGE);
        queueConnection.close();
    }

    @Test(timeout = 30000)
    public void testCreateTopicSessionOnTopicConnection() throws JMSException {
        TopicConnection topicConnection = createTopicConnectionToMockPulsarClient();
        topicConnection.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
        topicConnection.close();
    }
}
