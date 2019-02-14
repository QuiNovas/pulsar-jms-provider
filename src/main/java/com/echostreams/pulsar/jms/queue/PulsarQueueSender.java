package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.config.PulsarSession;
import com.echostreams.pulsar.jms.message.PulsarMessageProducer;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.*;

public class PulsarQueueSender extends PulsarMessageProducer implements QueueSender {

    public PulsarQueueSender(PulsarSession session, Queue queue, IntegerID senderId) throws JMSException {
        super(session, queue, senderId);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue)destination;
    }

    @Override
    public void send(Queue queue, Message message) throws JMSException {
        send((Destination)queue,message,defaultDeliveryMode,defaultPriority,defaultTimeToLive);
    }

    @Override
    public void send(Queue queue, Message message, int deliveryMode, int priority, long timeToLive) throws JMSException
    {
        send((Destination)queue, message, deliveryMode, priority, timeToLive);
    }

}
