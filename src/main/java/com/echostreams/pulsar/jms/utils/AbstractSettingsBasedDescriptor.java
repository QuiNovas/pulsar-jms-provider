package com.echostreams.pulsar.jms.utils;

public abstract class AbstractSettingsBasedDescriptor extends AbstractDescriptor {
    public AbstractSettingsBasedDescriptor() {
        super();
    }

    public AbstractSettingsBasedDescriptor(Settings settings) {
        super();
        initFromSettings(settings);
    }

    protected abstract void initFromSettings(Settings settings);

}
