package com.echostreams.pulsar.jms.client;

import javax.jms.*;
import java.io.Serializable;

public class PulsarJMSContext implements JMSContext {

    @Override
    public JMSContext createContext(int i) {
        return null;
    }

    @Override
    public JMSProducer createProducer() {
        return null;
    }

    @Override
    public String getClientID() {
        return null;
    }

    @Override
    public void setClientID(String s) {

    }

    @Override
    public ConnectionMetaData getMetaData() {
        return null;
    }

    @Override
    public ExceptionListener getExceptionListener() {
        return null;
    }

    @Override
    public void setExceptionListener(ExceptionListener exceptionListener) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void setAutoStart(boolean b) {

    }

    @Override
    public boolean getAutoStart() {
        return false;
    }

    @Override
    public void close() {

    }

    @Override
    public BytesMessage createBytesMessage() {
        return null;
    }

    @Override
    public MapMessage createMapMessage() {
        return null;
    }

    @Override
    public Message createMessage() {
        return null;
    }

    @Override
    public ObjectMessage createObjectMessage() {
        return null;
    }

    @Override
    public ObjectMessage createObjectMessage(Serializable serializable) {
        return null;
    }

    @Override
    public StreamMessage createStreamMessage() {
        return null;
    }

    @Override
    public TextMessage createTextMessage() {
        return null;
    }

    @Override
    public TextMessage createTextMessage(String s) {
        return null;
    }

    @Override
    public boolean getTransacted() {
        return false;
    }

    @Override
    public int getSessionMode() {
        return 0;
    }

    @Override
    public void commit() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void recover() {

    }

    @Override
    public JMSConsumer createConsumer(Destination destination) {
        return null;
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String s) {
        return null;
    }

    @Override
    public JMSConsumer createConsumer(Destination destination, String s, boolean b) {
        return null;
    }

    @Override
    public Queue createQueue(String s) {
        return null;
    }

    @Override
    public Topic createTopic(String s) {
        return null;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String s) {
        return null;
    }

    @Override
    public JMSConsumer createDurableConsumer(Topic topic, String s, String s1, boolean b) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String s) {
        return null;
    }

    @Override
    public JMSConsumer createSharedDurableConsumer(Topic topic, String s, String s1) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String s) {
        return null;
    }

    @Override
    public JMSConsumer createSharedConsumer(Topic topic, String s, String s1) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue) {
        return null;
    }

    @Override
    public QueueBrowser createBrowser(Queue queue, String s) {
        return null;
    }

    @Override
    public TemporaryQueue createTemporaryQueue() {
        return null;
    }

    @Override
    public TemporaryTopic createTemporaryTopic() {
        return null;
    }

    @Override
    public void unsubscribe(String s) {

    }

    @Override
    public void acknowledge() {

    }
}
