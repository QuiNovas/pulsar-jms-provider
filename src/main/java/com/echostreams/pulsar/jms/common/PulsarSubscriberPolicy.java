package com.echostreams.pulsar.jms.common;

public class PulsarSubscriberPolicy {
    public static final int SUBSCRIBER_POLICY_LOG = 1;
    public static final int SUBSCRIBER_POLICY_REPORT_TO_PRODUCER = 2;

    public static boolean isValid(int policy) {
        return policy >= 0 && policy < 4;
    }
}
