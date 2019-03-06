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
import java.util.concurrent.TimeUnit;

public class PulsarJMSConsumer implements JMSConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSConsumer.class);

    private Consumer consumer;
    private Destination destination;
    private PulsarJMSContext jmsContext;
    private String messageSelector;

    public PulsarJMSConsumer(Destination destination, String messageSelector, PulsarJMSContext jmsContext) throws JMSException {
        try {
            this.destination = destination;
            this.messageSelector = messageSelector;
            this.jmsContext = jmsContext;
            this.consumer = new ConsumerBuilderImpl((PulsarClientImpl) jmsContext.getClient(), Schema.STRING)
                    .topic(((PulsarDestination) destination).getName())
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("test-subcription")
                    .subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("PulsarJMSConsumer exception", e);
        }
    }

    @Override
    public String getMessageSelector() {
        return messageSelector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSRuntimeException {
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener messageListener) throws JMSRuntimeException {

    }

    @Override
    public Message receive() {
        return readMessages(-1, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message receive(long timeout) {
        // Configure for infinite wait when timeout is zero (JMS Spec)
        if (timeout == 0) {
            timeout = -1;
        }
        return readMessages(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message receiveNoWait() {
        return receive();
    }

    @Override
    public void close() {
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            LOGGER.error("Exception while closing consumer", e);
        }
    }

    @Override
    public <T> T receiveBody(Class<T> aClass) {
        return null;
    }

    @Override
    public <T> T receiveBody(Class<T> aClass, long l) {
        return null;
    }

    @Override
    public <T> T receiveBodyNoWait(Class<T> aClass) {
        return null;
    }

    private MessageListener listener = new MessageListener() {
        @Override
        public void onMessage(Message message) {
            // Denault noop listener
        }
    };

    private PulsarMessage readMessages(long timeout, TimeUnit timeUnit) {
        org.apache.pulsar.client.api.Message<byte[]> msg = null;
        PulsarMessage pulsarMessage = null;
        try {
            // Wait until a message is available
            while ((msg = consumer.receive()) != null) {
                pulsarMessage = (PulsarMessage) new ObjectSerializer().byteArrayToObject(msg);

                // Extract the message as a printable string and then log
                LOGGER.info("Received message='{}' with msg-id={}", pulsarMessage.getBody(pulsarMessage.getJMSType().getClass()), msg.getMessageId());

                // Acknowledge processing of the message so that it can be deleted

                consumer.acknowledge(msg);
            }
        } catch (PulsarClientException e) {
            LOGGER.error("PulsarClientException during receiving message", e);
        } catch (JMSException e) {
            LOGGER.error("JMSException during receiving message", e);
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
