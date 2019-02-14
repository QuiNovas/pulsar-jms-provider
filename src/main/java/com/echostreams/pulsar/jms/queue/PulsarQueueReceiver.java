package com.echostreams.pulsar.jms.queue;

import com.echostreams.pulsar.jms.PulsarJMSProvider;
import com.echostreams.pulsar.jms.message.PulsarMessageConsumer;
import com.echostreams.pulsar.jms.utils.id.IntegerID;

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueReceiver;

public class PulsarQueueReceiver extends PulsarMessageConsumer implements QueueReceiver {

    public PulsarQueueReceiver(PulsarJMSProvider pulsarJMSProvider, PulsarQueueSession session, Queue queue, String messageSelector, IntegerID receiverId) throws JMSException {
        super(pulsarJMSProvider, session, queue, messageSelector,false, receiverId, null);
    }

    @Override
    public Queue getQueue() throws JMSException {
        return (Queue)destination;
    }
}
