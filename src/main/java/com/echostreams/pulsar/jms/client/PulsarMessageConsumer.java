package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.message.PulsarMessage;
import com.echostreams.pulsar.jms.utils.ObjectSerializer;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class PulsarMessageConsumer implements MessageConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageConsumer.class);

    private Consumer consumer;
    private Destination destination;
    private MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message message) {
            // Denault noop listener
        }
    };

    /**
     * consumer config should define a group Id
     *
     * @throws JMSException
     */
    public PulsarMessageConsumer(Properties config, Destination destination, PulsarConnection connection) throws JMSException {
        try {
            this.destination = destination;
            this.consumer = new ConsumerBuilderImpl((PulsarClientImpl) connection.getClient(), Schema.STRING)
                    .topic(((PulsarDestination) destination).getName())
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("test-subcription")
                    .subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("", e);
        }
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#getMessageSelector()
     */
    @Override
    public String getMessageSelector() throws JMSException {
        // TODO Auto-generated method stub
        return null;
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#getMessageListener()
     */
    @Override
    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#setMessageListener(javax.jms.MessageListener)
     */
    @Override
    public void setMessageListener(MessageListener listener)
            throws JMSException {
        this.listener = listener;
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#receive()
     */
    @Override
    public Message receive() throws JMSException {
        return readMessages(0, TimeUnit.MILLISECONDS);
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#receive(long)
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        return readMessages(1000, TimeUnit.MILLISECONDS);
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#receiveNoWait()
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        return receive(0);
    }

    public void commit() {
        // NOOP
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#close()
     */
    @Override
    public void close() throws JMSException {
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            LOGGER.error("Exception while closing consumer", e);
        }
    }

    private PulsarMessage readMessages(int timeout, TimeUnit milliseconds) {
        org.apache.pulsar.client.api.Message<byte[]> msg = null;
        PulsarMessage pulsarMessage = null;
        try {
            // Wait until a message is available
            msg = consumer.receive(timeout, milliseconds);
            pulsarMessage = (PulsarMessage) new ObjectSerializer().byteArrayToObject(msg);

            // Extract the message as a printable string and then log
            LOGGER.info("Received message='{}' with msg-id={}", pulsarMessage.getBody(pulsarMessage.getJMSType().getClass()), msg.getMessageId());

            // Acknowledge processing of the message so that it can be deleted

            consumer.acknowledge(msg);
        } catch (PulsarClientException e) {
            LOGGER.error("Exception during receiving message", e);
        } catch (JMSException e) {
            LOGGER.error("Exception during receiving message", e);
        }
        return pulsarMessage;
    }

    void unsubscribe() throws JMSException {
        try {
            consumer.unsubscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("Exception during unsubscribe message", e);
        }
    }
}
