package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.client.PulsarDestination;
import org.apache.pulsar.client.api.Consumer;

import javax.jms.*;
import java.util.*;

public class PulsarMessageConsumer implements MessageConsumer {
    private Consumer<Message> consumer;
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
    public PulsarMessageConsumer(Properties config, Destination destination) throws JMSException {
        //TODO need to map with pulsar consumer
        consumer = new KafkaConsumer<String, Message>(config);
        this.destination = destination;
        consumer.subscribe(Arrays.asList(this.destination.getName()));
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
        subscribe();
        //TODO need to map with pulsar consumer
        ConsumerRecords<String, Message> records = null;
        while (null == records) {
            records = consumer.poll(0);
        }
        return process(records);
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#receive(long)
     */
    @Override
    public Message receive(long timeout) throws JMSException {
        // Map<"topic name", ConsumerRecords<"trace_id", PulsarMessage>>
        subscribe();
        //TODO need to map with pulsar consumer
        ConsumerRecords<String, Message> records = consumer.poll(timeout);
        return process(records);
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#receiveNoWait()
     */
    @Override
    public Message receiveNoWait() throws JMSException {
        return receive(0);
    }

    //private List<Message> process(ConsumerRecords<String, Message> consumerRecords) throws JMSException{
    private Message process(ConsumerRecords<String, Message> consumerRecords) throws JMSException {
        List<Message> results = new ArrayList<Message>();
        //TODO need to map with pulsar consumer
        Iterable<ConsumerRecord<String, Message>> records = consumerRecords.records(destination.getName());
        // user specific logic to process record
        //processedOffsets.put(record.partition(), record.offset());
        //records.
        for (ConsumerRecord<String, Message> record : records) {
            Message message = record.value();
            listener.onMessage(message);
            TopicPartition tp = new TopicPartition(record.topic(), record.partition());
            OffsetAndMetadata oam = new OffsetAndMetadata(record.offset());
            Map<TopicPartition, OffsetAndMetadata> map = new HashMap<TopicPartition, OffsetAndMetadata>();
            map.put(tp, oam);
            consumer.commitSync(map);
            unsubscribe();
            return message;
            //results.add(message);
        }
        return null;
    }

    public void commit() {
        // NOOP
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageConsumer#close()
     */
    @Override
    public void close() throws JMSException {
        unsubscribe();
        consumer.close();
    }

    void subscribe() throws JMSException {
        //TODO need to map with pulsar consumer
        consumer.subscribe(Arrays.asList(this.destination.getName()));
    }

    public void unsubscribe() throws JMSException {
        consumer.unsubscribe();
    }

}
