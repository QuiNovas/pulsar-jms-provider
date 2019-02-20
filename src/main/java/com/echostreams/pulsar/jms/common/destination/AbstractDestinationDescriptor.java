package com.echostreams.pulsar.jms.common.destination;

import com.echostreams.pulsar.jms.utils.AbstractSettingsBasedDescriptor;
import com.echostreams.pulsar.jms.utils.PulsarJMSException;
import com.echostreams.pulsar.jms.utils.Settings;
import com.echostreams.pulsar.jms.utils.StringRelatedUtils;

import javax.jms.JMSException;

public abstract class AbstractDestinationDescriptor extends AbstractSettingsBasedDescriptor implements DestinationDescriptorMBean {
    // Attributes
    protected String name;
    protected boolean temporary;
    protected int initialBlockCount;
    protected int maxNonPersistentMessages;

    public AbstractDestinationDescriptor() {
        super();
    }

    public AbstractDestinationDescriptor(Settings settings) {
        super(settings);
    }

    @Override
    protected void initFromSettings(Settings settings) {
        this.name = settings.getStringProperty("name");
        this.temporary = settings.getBooleanProperty("temporary", false);
        this.initialBlockCount = settings.getIntProperty("persistentStore.initialBlockCount", 0);
        this.maxNonPersistentMessages = settings.getIntProperty("memoryStore.maxMessages", 0);
    }

    protected void copyAttributesTo(AbstractDestinationDefinition target) {
        target.initialBlockCount = initialBlockCount;
        target.maxNonPersistentMessages = maxNonPersistentMessages;
    }

    /**
     * Serialize the definition to settings
     */
    public final Settings asSettings() {
        Settings settings = new Settings();
        fillSettings(settings);
        return settings;
    }

    protected void fillSettings(Settings settings) {
        settings.setStringProperty("name", name);
        settings.setBooleanProperty("temporary", temporary);
        settings.setIntProperty("persistentStore.initialBlockCount", initialBlockCount);
        settings.setIntProperty("memoryStore.maxMessages", maxNonPersistentMessages);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public boolean isTemporary() {
        return temporary;
    }

    @Override
    public int getInitialBlockCount() {
        return initialBlockCount;
    }

    @Override
    public int getMaxNonPersistentMessages() {
        return maxNonPersistentMessages;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setTemporary(boolean temporary) {
        this.temporary = temporary;
    }

    public void setInitialBlockCount(int initialBlockCount) {
        this.initialBlockCount = initialBlockCount;
    }

    public void setMaxNonPersistentMessages(int maxNonPersistentMessages) {
        this.maxNonPersistentMessages = maxNonPersistentMessages;
    }

    @Override
    public void check() throws JMSException {
        if (StringRelatedUtils.isEmpty(name))
            throw new PulsarJMSException("Missing descriptor property : name", "INVALID_DESCRIPTOR");
    }

}
