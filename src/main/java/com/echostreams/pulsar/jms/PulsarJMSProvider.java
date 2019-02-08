package com.echostreams.pulsar.jms;

import com.echostreams.pulsar.jms.utils.PropertiesUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PulsarJMSProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSProvider.class);

    public static void main(String[] args) {
        PropertiesUtils propertiesUtils = PropertiesUtils.getDataFromPropFile();
        propertiesUtils.getServiceUrl();
    }
}
