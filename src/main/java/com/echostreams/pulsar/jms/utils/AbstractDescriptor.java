package com.echostreams.pulsar.jms.utils;

import java.io.File;

public abstract class AbstractDescriptor implements Checkable {
    private File descriptorFile;

    public AbstractDescriptor() {
    }

    public File getDescriptorFile() {
        return descriptorFile;
    }

    public void setDescriptorFile(File descriptorFile) {
        this.descriptorFile = descriptorFile;
    }
}
