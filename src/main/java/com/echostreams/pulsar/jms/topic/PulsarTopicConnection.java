package com.echostreams.pulsar.jms.topic;

import com.echostreams.pulsar.jms.config.PulsarConnection;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.*;

public class PulsarTopicConnection extends PulsarConnection implements TopicConnection {

    public PulsarTopicConnection(String clientID) {
        super(clientID);
    }

    @Override
    public TopicSession createTopicSession(boolean transacted, int acknowledgeMode) throws JMSException {
        isClosed();
        PulsarTopicSession session =  new PulsarTopicSession(idProvider.createID(),this,pulsarJMSProvider,transacted,acknowledgeMode);
        registerSession(session);
        return session;
    }

    @Override
    public ConnectionConsumer createConnectionConsumer(Topic topic, String s, ServerSessionPool serverSessionPool, int i) throws JMSException {
        isClosed();
        throw new PulsarJMSException("Unsupported feature","UNSUPPORTED_FEATURE");
    }
}
