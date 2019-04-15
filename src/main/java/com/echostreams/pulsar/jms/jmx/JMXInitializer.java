package com.echostreams.pulsar.jms.jmx;

import com.echostreams.pulsar.jms.client.PulsarQueue;
import com.echostreams.pulsar.jms.client.PulsarTopic;
import com.echostreams.pulsar.jms.config.PulsarConfig;
import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.jmx.html.PulsarHtmlAdaptorJMXAgent;
import com.echostreams.pulsar.jms.jmx.local.PulsarLocalJMXAgent;
import com.echostreams.pulsar.jms.jmx.rmi.PulsarRmiJMXAgent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import javax.management.MBeanServerFactory;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class JMXInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JMXInitializer.class);

    public static JMXInitializer jmxInitializer;

    public static PulsarJMXAgent jmxAgent;

    public static synchronized void initializeJMXAgent() {
        if (jmxInitializer == null) {
            jmxInitializer = new JMXInitializer();
        }

    }

    public JMXInitializer() {
        if (PulsarConfig.pulsarConfig == null) {
            PulsarConfig.initializeConfig(PulsarConstants.DEFAULT_CONFIG_FILE);
        }

        try {
            if (Boolean.TRUE.equals(PulsarConfig.JMX_AGENT_ENABLED)) {
                jmxAgent = new PulsarRmiJMXAgent(ManagementFactory.getPlatformMBeanServer(), PulsarConstants.APP_NAME, PulsarConfig.JMX_JNDI_RMI_PORT, PulsarConfig.JMX_RMI_LISTEN_ADDR);
            } else if (PulsarConstants.JMX_LOCAL.equals(PulsarConfig.JMX_AGENT_ENABLED)) {
                jmxAgent = new PulsarLocalJMXAgent(ManagementFactory.getPlatformMBeanServer()); // Use a local-only agent(accessed from same jvm or by jconsole/jvisualvm
            } else if (PulsarConstants.JMX_HTML.equals(PulsarConfig.JMX_AGENT_ENABLED)) {
                jmxAgent = new PulsarHtmlAdaptorJMXAgent(MBeanServerFactory.createMBeanServer(), PulsarConfig.JMX_JNDI_RMI_PORT); // Use a html adaptor(accessed from web browser
            }
        } catch (JMSException e) {
            LOGGER.error(" ");
        }
    }

    public static void registerQueue(PulsarQueue pulsarQueue) {
        try {
            if (jmxAgent != null)
                jmxAgent.registerMBean(new ObjectName(pulsarQueue.getClass().getPackage().getName() + ":type=Queues,name=" + pulsarQueue.getName()), pulsarQueue);
        } catch (Exception e) {
            LOGGER.error("Cannot register queue on JMX agent", e);
        }
    }

    public void unRegisterQueue(PulsarQueue pulsarQueue) {
        try {
            if (jmxAgent != null)
                jmxAgent.unregisterMBean(new ObjectName(pulsarQueue.getClass().getPackage().getName() + ":type=Queues,name=" + pulsarQueue.getName()));
        } catch (Exception e) {
            LOGGER.error("Cannot Unregister queue on JMX agent", e);
        }
    }

    public static void registerTopic(PulsarTopic pulsarTopic) {
        try {
            if (jmxAgent != null)
                jmxAgent.registerMBean(new ObjectName(pulsarTopic.getClass().getPackage().getName() + ":type=Topics,name=" + pulsarTopic.getName()), pulsarTopic);
        } catch (Exception e) {
            LOGGER.error("Cannot register topic on JMX agent", e);
        }
    }

    public void unRegisterTopic(PulsarTopic pulsarTopic) {
        try {
            if (jmxAgent != null)
                jmxAgent.unregisterMBean(new ObjectName(pulsarTopic.getClass().getPackage().getName() + ":type=Topics,name=" + pulsarTopic.getName()));
        } catch (Exception e) {
            LOGGER.error("Cannot Unregister topic on JMX agent", e);
        }
    }
}
