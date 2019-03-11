package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.message.PulsarMessage;
import com.echostreams.pulsar.jms.utils.ObjectSerializer;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.impl.ConsumerBuilderImpl;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import javax.jms.Message;
import javax.jms.MessageListener;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class PulsarMessageConsumer implements MessageConsumer, QueueReceiver, TopicSubscriber {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarMessageConsumer.class);

    private Consumer<byte[]> consumer;
    private Destination destination;
    private PulsarSession session;
    private String messageSelector;
    private boolean durable;
    private boolean noLocal;
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
    public PulsarMessageConsumer(Destination destination, String messageSelector, PulsarSession session) throws JMSException {
        try {
            this.destination = destination;
            this.messageSelector = messageSelector;
            this.session = session;
            this.consumer = new ConsumerBuilderImpl((PulsarClientImpl) session.getConnection().getClient(), Schema.BYTES)
                    .topic(((PulsarDestination) destination).getName())
                    .subscriptionType(SubscriptionType.Shared)
                    .subscriptionName("test-subcription")
                    .subscribe();
        } catch (PulsarClientException e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public String getMessageSelector() throws JMSException {
        // TODO Auto-generated method stub
        return this.messageSelector;
    }

    @Override
    public MessageListener getMessageListener() throws JMSException {
        return listener;
    }

    @Override
    public void setMessageListener(MessageListener listener)
            throws JMSException {
        this.listener = listener;
    }

    @Override
    public Message receive() throws JMSException {
        return readMessages(0, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message receive(long timeout) throws JMSException {
        return readMessages(timeout, TimeUnit.MILLISECONDS);
    }

    @Override
    public Message receiveNoWait() throws JMSException {
        return receive(0);
    }

    public void commit() {
        // NOOP
    }

    @Override
    public void close() throws JMSException {
        try {
            consumer.close();
        } catch (PulsarClientException e) {
            LOGGER.error("Exception while closing consumer", e);
        }
    }

    @Override
    public Queue getQueue() throws JMSException {
        return null;
    }

    @Override
    public Topic getTopic() throws JMSException {
        return null;
    }

    @Override
    public boolean getNoLocal() throws JMSException {
        return false;
    }

    private PulsarMessage readMessages(long timeout, TimeUnit timeUnit) {
        org.apache.pulsar.client.api.Message<byte[]> msg = null;
        PulsarMessage pulsarMessage = null;
        try {
            msg = consumer.receive();
            pulsarMessage = (PulsarMessage) new ObjectSerializer().byteArrayToObject(msg);
            pulsarMessage.setJMSMessageID(msg.getMessageId().toString());
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

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public void setNoLocal(boolean noLocal) {
        this.noLocal = noLocal;
    }

    public PulsarMessage receiveAsync() throws ExecutionException, InterruptedException {
        PulsarMessage pulsarMessage = null;
        CompletableFuture<org.apache.pulsar.client.api.Message<byte[]>> asyncMessage = consumer.receiveAsync();
        if (asyncMessage != null) {
            pulsarMessage = (PulsarMessage) new ObjectSerializer().byteArrayToObject(asyncMessage.get());
        }
        return pulsarMessage;
    }
}
