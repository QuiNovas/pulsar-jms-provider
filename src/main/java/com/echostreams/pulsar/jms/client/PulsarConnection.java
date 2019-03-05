package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.common.PulsarConnectionMetaDataImpl;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.jms.*;
import java.util.Properties;

public class PulsarConnection implements Connection, QueueConnection, TopicConnection {

    private static ConnectionMetaData metaData = new PulsarConnectionMetaDataImpl();

    private Properties config;
    private Session session;
    private PulsarClient client;

    public PulsarConnection(PulsarClient client) {
        this.client = client;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createSession(boolean, int)
     */
    @Override
    public Session createSession(boolean transacted, int acknowledgeMode)
            throws JMSException {
        session = new PulsarSession(this, transacted, acknowledgeMode);
        return session;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createSession(int)
     */
    @Override
    public Session createSession(int sessionMode) throws JMSException {
        return createSession(true, sessionMode);
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createSession()
     */
    @Override
    public Session createSession() throws JMSException {
        return createSession(true, Session.AUTO_ACKNOWLEDGE);
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#getClientID()
     */
    @Override
    public String getClientID() throws JMSException {
        //return (String) config.get(ProducerConfiguration.CLIENT_ID_CONFIG);
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#setClientID(java.lang.String)
     */
    @Override
    public void setClientID(String clientID) throws JMSException {
        //config.put(ProducerConfig.CLIENT_ID_CONFIG, clientID);
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#getMetaData()
     */
    @Override
    public ConnectionMetaData getMetaData() throws JMSException {
        return metaData;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#getExceptionListener()
     */
    @Override
    public ExceptionListener getExceptionListener() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#setExceptionListener(javax.jms.ExceptionListener)
     */
    @Override
    public void setExceptionListener(ExceptionListener listener)
            throws JMSException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#start()
     */
    @Override
    public void start() throws JMSException {
        // TODO create session pool
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#stop()
     */
    @Override
    public void stop() throws JMSException {
        close();
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#close()
     */
    @Override
    public void close() throws JMSException {
        session.close();
        try {
            client.close();
        } catch (PulsarClientException e) {
            throw new PulsarJMSException("Closing connection exception", e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createConnectionConsumer(javax.jms.Destination, java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createConnectionConsumer(Destination destination,
                                                       String messageSelector, ServerSessionPool sessionPool,
                                                       int maxMessages) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createSharedConnectionConsumer(javax.jms.Topic, java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createSharedConnectionConsumer(Topic topic,
                                                             String subscriptionName, String messageSelector,
                                                             ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (TopicSession) createSession(transacted, acknowledgeMode);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }

    /* (non-Javadoc)
         * @see javax.jms.Connection#createDurableConnectionConsumer(javax.jms.Topic, java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
         */
    @Override
    public ConnectionConsumer createDurableConnectionConsumer(Topic topic,
                                                              String subscriptionName, String messageSelector,
                                                              ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.Connection#createSharedDurableConnectionConsumer(javax.jms.Topic, java.lang.String, java.lang.String, javax.jms.ServerSessionPool, int)
     */
    @Override
    public ConnectionConsumer createSharedDurableConnectionConsumer(
            Topic topic, String subscriptionName, String messageSelector,
            ServerSessionPool sessionPool, int maxMessages) throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public QueueSession createQueueSession(boolean transacted, int acknowledgeMode) throws JMSException {
        return (QueueSession) createSession(transacted, acknowledgeMode);
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Queue queue, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        return null;
    }

    public PulsarClient getClient() {
        return client;
    }

    public void setClient(PulsarClient client) {
        this.client = client;
    }
}
