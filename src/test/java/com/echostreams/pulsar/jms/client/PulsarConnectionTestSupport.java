package com.echostreams.pulsar.jms.client;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;

import static org.mockito.Mockito.mock;

public class PulsarConnectionTestSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarConnectionTestSupport.class);

    protected PulsarConnection connection;


    @Rule
    public TestName _testName = new TestName();

    private PulsarClient createMockPulsarClient() {
        return mock(PulsarClientImpl.class);
    }

    protected PulsarConnection createConnectionToMockPulsarClient() {
        return createConnection();
    }

    protected QueueConnection createQueueConnectionToMockPulsarClient() {
        return createConnection();
    }

    protected TopicConnection createTopicConnectionToMockPulsarClient() {
        return createConnection();
    }

    private PulsarConnection createConnection() {
        return new PulsarConnection(createMockPulsarClient());
    }

    protected PulsarJMSContext createContextToMockPulsarClient() throws JMSException {
        return new PulsarJMSContext(createMockPulsarClient(), JMSContext.AUTO_ACKNOWLEDGE);
    }

    @Before
    public void setUp() throws Exception {
        LOGGER.info("========== start " + getTestName() + " ==========");
    }

    @After
    public void tearDown() throws Exception {
        LOGGER.info("========== tearDown " + getTestName() + " ==========");
    }

    protected String getTestName() {
        return getClass().getSimpleName() + "." + _testName.getMethodName();
    }

}
