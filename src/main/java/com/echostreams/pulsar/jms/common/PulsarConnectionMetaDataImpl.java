package com.echostreams.pulsar.jms.common;

import java.util.Enumeration;
import java.util.NoSuchElementException;

public class PulsarConnectionMetaDataImpl implements javax.jms.ConnectionMetaData {
    @Override
    public int getJMSMajorVersion() {
        return PulsarJMSProviderVersion.getJMSMajorVersion();
    }

    @Override
    public int getJMSMinorVersion() {
        return PulsarJMSProviderVersion.getJMSMinorVersion();
    }

    @Override
    public String getJMSProviderName() {
        return "Apache Pulsar";
    }

    @Override
    public String getJMSVersion() {
        return PulsarJMSProviderVersion.getJMSMajorVersion() + "." + PulsarJMSProviderVersion.getJMSMinorVersion();
    }

    @Override
    public Enumeration<Object> getJMSXPropertyNames() {
        return new Enumeration<Object>() {

            @Override
            public boolean hasMoreElements() {
                return false;
            }

            @Override
            public Object nextElement() {
                throw new NoSuchElementException();
            }
        };
    }

    @Override
    public int getProviderMajorVersion() {
        return PulsarJMSProviderVersion.getProviderMajorVersion();
    }

    @Override
    public int getProviderMinorVersion() {
        return PulsarJMSProviderVersion.getProviderMinorVersion();
    }

    @Override
    public String getProviderVersion() {
        return PulsarJMSProviderVersion.getProviderMajorVersion() + "." + PulsarJMSProviderVersion.getProviderMinorVersion();
    }
}
