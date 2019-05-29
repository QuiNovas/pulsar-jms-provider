package com.echostreams.pulsar.jms.jmx.html;

import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.jmx.AbstractPulsarJMXAgent;
import com.sun.jdmk.comm.HtmlAdaptorServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.*;

public class PulsarHtmlAdaptorJMXAgent extends AbstractPulsarJMXAgent {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarHtmlAdaptorJMXAgent.class);

    // Attributes
    private int port;

    public PulsarHtmlAdaptorJMXAgent(MBeanServer mBeanServer, int port) {
        super(mBeanServer);
        this.port = port;
        init();
    }

    private void init() {
        ObjectName adapterName;
        HtmlAdaptorServer adaptor = null;
        try {
            adapterName = new ObjectName("com.echostreams.pulsar.jms.jmx.html:name=PulsarHtmlAdaptorJMXAgent,port=" + port);
            adaptor = new HtmlAdaptorServer(port);
            mBeanServer.registerMBean(adaptor, adapterName);
        } catch (MalformedObjectNameException e) {
            LOGGER.error("Error while preparing for HtmlAdaptor JMX Agent", e);
            System.exit(-1);
        } catch (InstanceAlreadyExistsException e) {
            LOGGER.error("Error while preparing for HtmlAdaptor JMX Agent", e);
            System.exit(-1);
        } catch (MBeanRegistrationException e) {
            LOGGER.error("Error while preparing for HtmlAdaptor JMX Agent", e);
            System.exit(-1);
        } catch (NotCompliantMBeanException e) {
            LOGGER.error("Error while preparing for HtmlAdaptor JMX Agent", e);
            System.exit(-1);
        }
        adaptor.start();
    }

    @Override
    protected String getType() {
        return PulsarConstants.JMX_HTML;
    }
}
