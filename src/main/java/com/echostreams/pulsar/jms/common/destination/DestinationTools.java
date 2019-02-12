package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.config.PulsarConfiguration;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;

import javax.jms.*;

public final class DestinationTools {
    /**
     * Make sure the given destination is a light-weight serializable destination reference
     */
    public static DestinationRef asRef(Destination destination) throws JMSException {
        if (destination == null)
            return null;

        if (destination instanceof DestinationRef)
            return (DestinationRef) destination;

        if (destination instanceof Queue)
            return new QueueRef(((Queue) destination).getQueueName());

        if (destination instanceof Topic)
            return new TopicRef(((Topic) destination).getTopicName());

        throw new InvalidDestinationException("Unsupported destination type : " + destination, "INVALID_DESTINATION");
    }

    /**
     * Make sure the given destination is a light-weight serializable destination reference
     */
    public static Queue asRef(Queue queue) throws JMSException {
        if (queue == null)
            return null;

        if (queue instanceof QueueRef)
            return queue;

        return new QueueRef(queue.getQueueName());
    }

    /**
     * Make sure the given destination is a light-weight serializable destination reference
     */
    public static Topic asRef(Topic topic) throws JMSException {
        if (topic == null)
            return null;

        if (topic instanceof TopicRef)
            return topic;

        return new TopicRef(topic.getTopicName());
    }

    /**
     * Get a queue name for a given topic consumer
     */
    public static String getQueueNameForTopicConsumer(String topicName, String consumerID) {
        return topicName + "-" + consumerID;
    }

    private static void checkDestinationName(String destinationName) throws JMSException {
        for (int i = 0; i < destinationName.length(); i++) {
            char c = destinationName.charAt(i);
            if (c >= 'a' && c <= 'z')
                continue;
            if (c >= 'A' && c <= 'Z')
                continue;
            if (c >= '0' && c <= '9')
                continue;
            if (c == '_' || c == '-')
                continue;

            throw new PulsarJMSException("Destination name '" + destinationName + "' contains an invalid character : " + c, "INVALID_DESTINATION_NAME");
        }
    }

    /**
     * Check the validity of a queue name
     */
    public static void checkQueueName(String queueName) throws JMSException {
        if (queueName == null)
            throw new PulsarJMSException("Queue name is not set", "INVALID_DESTINATION_NAME");
        if (queueName.length() > PulsarConfiguration.MAX_QUEUE_NAME_SIZE)
            throw new PulsarJMSException("Queue name '" + queueName + "' is too long (" + queueName.length() + " > " + PulsarConfiguration.MAX_QUEUE_NAME_SIZE + ")", "INVALID_DESTINATION_NAME");
        checkDestinationName(queueName);
    }

    /**
     * Check the validity of a topic name
     */
    public static void checkTopicName(String topicName) throws JMSException {
        if (topicName == null)
            throw new PulsarJMSException("Topic name is not set", "INVALID_DESTINATION_NAME");
        if (topicName.length() > PulsarConfiguration.MAX_TOPIC_NAME_SIZE)
            throw new PulsarJMSException("Topic name '" + topicName + "' is too long (" + topicName.length() + " > " + PulsarConfiguration.MAX_TOPIC_NAME_SIZE + ")", "INVALID_DESTINATION_NAME");
        checkDestinationName(topicName);
    }
}   
