package com.echostreams.pulsar.jms.config;

public abstract class PulsarConfiguration {

    private static final String brokerUrl = "http://localhost:8080";//"myFactoryLookup";

    public static String getBrokerUrl() {
        return brokerUrl;
    }
}
