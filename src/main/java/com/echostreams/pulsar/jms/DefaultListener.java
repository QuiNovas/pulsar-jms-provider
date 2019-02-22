package com.echostreams.pulsar.jms;

import org.apache.pulsar.client.api.Consumer;

import javax.jms.Message;
import javax.jms.MessageListener;

public class DefaultListener implements MessageListener {
    private Consumer<Message> consumer;

    public DefaultListener(Consumer<Message> consumer) {
        this.consumer = consumer;
    }

    /* (non-Javadoc)
     * @see javax.jms.MessageListener#onMessage(javax.jms.Message)
     */
    @Override
    public void onMessage(Message message) {
        // TODO Auto-generated method stub

    }

}
