package com.echostreams.pulsar.jms.utils;

import com.echostreams.pulsar.jms.client.PulsarDestination;
import com.echostreams.pulsar.jms.client.PulsarQueue;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

public class DestinationUtils {

    public static PulsarDestination transformDestination(Destination destination) throws JMSException {
        if (destination == null) {
            return null;
        }
        if (destination instanceof Queue) {
            return new PulsarQueue(((Queue) destination).getQueueName());
        }
        if (destination instanceof Topic) {
            return new PulsarQueue(((Topic) destination).getTopicName());
        }
        if (destination instanceof PulsarDestination) {
            return (PulsarDestination) destination;
        }
        //TODO need to add for temporary if needed
        throw new JMSException("Could not support non-OpenJMS destination into a Pulsar destination: " + destination);
    }

}

