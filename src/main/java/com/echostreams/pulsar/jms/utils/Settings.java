package com.echostreams.pulsar.jms.utils;

import javax.jms.JMSException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;
import java.util.Set;

/**
 * Settings
 * <p>This a type-enabled wrapper for a Properties object</p>
 */
public final class Settings {
    private Properties settings = new Properties();

    public Settings() {

    }

    public Settings(Properties settings) {
        this.settings.putAll(settings);
    }

    public void readFrom(File settingsFile) throws JMSException {
        FileInputStream in = null;
        try {
            in = new FileInputStream(settingsFile);
            settings.load(in);
            in.close();
        } catch (Exception e) {
            throw new PulsarJMSException("Cannot load settings from " + settingsFile.getAbsolutePath() + " : " + e, "FS_ERROR");
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (Exception e) {
                throw new PulsarJMSException("Cannot close settings file " + settingsFile.getAbsolutePath() + " : " + e, "FS_ERROR");
            }
        }
    }

    public void writeTo(File settingsFile, String title) throws JMSException {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(settingsFile);
            settings.store(out, title);
            out.close();
        } catch (Exception e) {
            throw new PulsarJMSException("Cannot save settings to " + settingsFile.getAbsolutePath() + " : " + e, "FS_ERROR");
        } finally {
            try {
                if (out != null)
                    out.close();
            } catch (Exception e) {
                throw new PulsarJMSException("Cannot close settings file " + settingsFile.getAbsolutePath() + " : " + e, "FS_ERROR");
            }
        }
    }

    public String getStringProperty(String key) {
        return getStringProperty(key, null, true);
    }

    public String getStringProperty(String key, String defaultValue) {
        return getStringProperty(key, defaultValue, true);
    }

    public String getStringProperty(String key, String defaultValue, boolean replaceSystemProperties) {
        String value = settings.getProperty(key, defaultValue);
        if (replaceSystemProperties)
            value = SystemTools.replaceSystemProperties(value);
        return value != null ? value.trim() : null;
    }

    public int getIntProperty(String key, int defaultValue) {
        String value = SystemTools.replaceSystemProperties(settings.getProperty(key));
        if (value == null)
            return defaultValue;
        try {
            return Integer.parseInt(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public long getLongProperty(String key, long defaultValue) {
        String value = SystemTools.replaceSystemProperties(settings.getProperty(key));
        if (value == null)
            return defaultValue;
        try {
            return Long.parseLong(value);
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = SystemTools.replaceSystemProperties(settings.getProperty(key));
        if (value == null)
            return defaultValue;
        return Boolean.valueOf(value.trim()).booleanValue();
    }

    public void setStringProperty(String key, String value) {
        settings.setProperty(key, value);
    }

    public void setIntProperty(String key, int value) {
        settings.setProperty(key, String.valueOf(value));
    }

    public void setLongProperty(String key, long value) {
        settings.setProperty(key, String.valueOf(value));
    }

    public void setBooleanProperty(String key, boolean value) {
        settings.setProperty(key, String.valueOf(value));
    }

    public String removeProperty(String key) {
        return (String) settings.remove(key);
    }

    public Properties asProperties() {
        return settings;
    }

    public Set<Object> keySet() {
        return settings.keySet();
    }
}
