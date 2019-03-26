package com.echostreams.pulsar.jms.client;

import com.echostreams.pulsar.jms.config.PulsarConfig;
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
import java.util.concurrent.*;

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
            if (PulsarConfig.consumerConfig == null) {
                this.consumer = new ConsumerBuilderImpl((PulsarClientImpl) jmsContext.getClient(), Schema.BYTES)
                        .topic(((PulsarDestination) destination).getName())
                        .subscriptionType(SubscriptionType.Shared)
                        .subscriptionName("test-subcription")
                        .subscribe();
            } else {
                this.consumer = PulsarConfig.consumerConfig.topic(((PulsarDestination) destination).getName()).subscribe();
            }
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

        // Submitting a Callable task to an ExecutorService and getting the result via a Future object.
        ExecutorService executorService = Executors.newFixedThreadPool(2);
        Future<PulsarMessage> pulsarMessageFuture = executorService.submit(new PulsarJMSConsumerThread(timeout, timeUnit));

        PulsarMessage pulsarMessage = null;
        try {

            pulsarMessage = pulsarMessageFuture.get();

        } catch (InterruptedException | ExecutionException ex) {
            ex.printStackTrace();
        }

        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(800, TimeUnit.MILLISECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
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

    /*
     * Concurrently consume the pulsar message and return it back
     */
    private class PulsarJMSConsumerThread implements Callable<PulsarMessage> {
        private final long timeOut;
        private final TimeUnit timeUnit;

        private PulsarJMSConsumerThread(long timeOut, TimeUnit timeUnit) {
            this.timeOut = timeOut;
            this.timeUnit = timeUnit;
        }

        @Override
        public PulsarMessage call() {
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
    }

}
