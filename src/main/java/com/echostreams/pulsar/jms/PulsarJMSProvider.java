package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.utils.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.naming.NamingException;

public class PulsarJMSProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSProvider.class);

    public static void main(String[] args) throws NamingException, JMSException, InterruptedException {
        PropertiesUtils propertiesUtils = PropertiesUtils.getDataFromPropFile();
        propertiesUtils.getServiceUrl();





    }

    public void deleteQueue(String queueName) {

    }

    public void deleteTopic(String topicName) {

    }
}
