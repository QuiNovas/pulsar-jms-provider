package com.echostreams.pulsar.jms.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

/**
 * PulsarJMSProviderVersion
 */
public class PulsarJMSProviderVersion {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMSProviderVersion.class);

    // Provider version
    private static int PROVIDER_MAJOR_VERSION = 0;
    private static int PROVIDER_MINOR_VERSION = 0;
    private static String PROVIDER_RELEASE_VERSION = "dev";

    // Supported JMS level
    private static final int JMS_MAJOR_VERSION = 2;
    private static final int JMS_MINOR_VERSION = 1;

    static {
        try {
            InputStream in = PulsarJMSProviderVersion.class.getResourceAsStream("/PulsarJMSProvider.version");
            if (in != null) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(in));
                String versionString = reader.readLine();
                reader.close();
                if (versionString != null && !versionString.startsWith("$")) {
                    StringTokenizer st = new StringTokenizer(versionString, ".");
                    int majorVersion = Integer.parseInt(st.nextToken());
                    int minorVersion = Integer.parseInt(st.nextToken());
                    String releaseVersion = st.nextToken();

                    PROVIDER_MAJOR_VERSION = majorVersion;
                    PROVIDER_MINOR_VERSION = minorVersion;
                    PROVIDER_RELEASE_VERSION = releaseVersion;
                }
            }
        } catch (Exception e) {
            LOGGER.warn("Could not retrieve FFMQ version information : " + e.toString());
        }
    }

    /**
     * Get the major version number of the provider
     *
     * @return the major version number of the provider
     */
    public static int getProviderMajorVersion() {
        return PROVIDER_MAJOR_VERSION;
    }

    /**
     * Get the minor version number of the provider
     *
     * @return the minor version number of the provider
     */
    public static int getProviderMinorVersion() {
        return PROVIDER_MINOR_VERSION;
    }

    /**
     * Get the release version string of the provider
     *
     * @return the minor version string of the provider
     */
    public static String getProviderReleaseVersion() {
        return PROVIDER_RELEASE_VERSION;
    }

    /**
     * Get the minor version number of the supported JMS specification
     *
     * @return the minor version number of the supported JMS specification
     */
    public static int getJMSMajorVersion() {
        return JMS_MAJOR_VERSION;
    }

    /**
     * Get the major version number of the supported JMS specification
     *
     * @return the major version number of the supported JMS specification
     */
    public static int getJMSMinorVersion() {
        return JMS_MINOR_VERSION;
    }

}
