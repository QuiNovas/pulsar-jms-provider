package com.echostreams.pulsar.jms.utils.id;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Random;

public final class UUIDProvider {
    private static final Logger LOGGER = LoggerFactory.getLogger(UUIDProvider.class);

    private static UUIDProvider instance = null;

    public static synchronized UUIDProvider getInstance() {
        if (instance == null)
            instance = new UUIDProvider();

        return instance;
    }

    private Random seed;
    private String fixedPart;

    private UUIDProvider() {
        try {
            this.seed = new MTRandom();

            // Try to find localhost address
            byte[] ifBytes = null;
            InetAddress inetaddress = InetAddress.getLocalHost();
            ifBytes = inetaddress != null ? inetaddress.getAddress() : null;
            if (ifBytes == null) {
                // Cannot determine local node address,
                // use a random value instead
                LOGGER.warn("Cannot determine localhost address, falling back to random value ...");
                ifBytes = new byte[4];
                seed.nextBytes(ifBytes);
            }

            StringBuilder base = new StringBuilder();

            String s = hexFormat(getInt(ifBytes));
            String s1 = hexFormat(hashCode());
            base.append("-");
            base.append(s.substring(0, 4));
            base.append("-");
            base.append(s.substring(4));
            base.append("-");
            base.append(s1.substring(0, 4));
            base.append("-");
            base.append(s1.substring(4));
            fixedPart = base.toString();
            seed.nextInt();
        } catch (Exception e) {
            LOGGER.error("Could not initialise UUID generator", e);
            throw new IllegalStateException("Could not initialise UUID generator : " + e.getMessage());
        }
    }

    public String getUUID() {
        StringBuilder uuid = new StringBuilder(36);
        int i = (int) System.currentTimeMillis();
        int j = seed.nextInt();
        hexFormat(i, uuid);
        uuid.append(fixedPart);
        hexFormat(j, uuid);
        return uuid.toString();
    }

    public String getShortUUID() {
        StringBuilder uuid = new StringBuilder(16);
        int i = (int) System.currentTimeMillis();
        int j = seed.nextInt();
        hexFormat(i, uuid);
        hexFormat(j, uuid);
        return uuid.toString();
    }

    private static int getInt(byte abyte0[]) {
        int i = 0;
        int j = 24;
        for (int k = 0; j >= 0; k++) {
            int l = abyte0[k] & 0xff;
            i += l << j;
            j -= 8;
        }

        return i;
    }

    private static String hexFormat(int i) {
        StringBuilder sb = new StringBuilder(8);
        hexFormat(i, sb);
        return sb.toString();
    }

    private static void hexFormat(int i, StringBuilder uuid) {
        String s = Integer.toHexString(i);
        for (int n = 0; n < 8 - s.length(); n++)
            uuid.append("0");
        uuid.append(s);
    }
}
