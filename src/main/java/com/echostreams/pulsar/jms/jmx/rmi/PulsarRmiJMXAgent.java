package com.echostreams.pulsar.jms.jmx.rmi;

import com.echostreams.pulsar.jms.config.PulsarConstants;
import com.echostreams.pulsar.jms.exceptions.PulsarJMSException;
import com.echostreams.pulsar.jms.jmx.AbstractPulsarJMXAgent;

import javax.jms.JMSException;
import javax.management.MBeanServer;
import javax.management.remote.JMXConnectorServer;
import javax.management.remote.JMXConnectorServerFactory;
import javax.management.remote.JMXServiceURL;
import javax.management.remote.rmi.RMIConnectorServer;
import java.net.InetAddress;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.RMIServerSocketFactory;
import java.util.HashMap;
import java.util.Map;

public class PulsarRmiJMXAgent extends AbstractPulsarJMXAgent {

    // Attributes
    private String agentName;
    private int jndiRmiPort;
    private String rmiListenAddr;

    // Runtime
    private JMXConnectorServer connectorServer;
    private PulsarJMXRMIServerSocketFactory mBeanServerSocketFactory;
    private Registry registry;

    public PulsarRmiJMXAgent(MBeanServer mBeanServer, String agentName, int jndiRmiPort, String rmiListenAddr) throws JMSException {
        super(mBeanServer);
        this.agentName = agentName;
        this.jndiRmiPort = jndiRmiPort;
        this.rmiListenAddr = rmiListenAddr;
        init();
    }

    private void init() throws JMSException {
        try {
            // Get or create an RMI registry
            if (rmiListenAddr == null || rmiListenAddr.equals("auto"))
                rmiListenAddr = InetAddress.getLocalHost().getHostName();

            // Connector JNDI name
            String jndiName = "jmxconnector-" + agentName;

            try {
                registry = LocateRegistry.getRegistry(rmiListenAddr, jndiRmiPort);
                registry.lookup(jndiName);

                // Remove the old registered connector
                registry.unbind(jndiName);

                LOGGER.debug("RMI registry found at " + rmiListenAddr + ":" + jndiRmiPort + " with connector already registered");
            } catch (NotBoundException e) {
                // Registry already exists
                LOGGER.debug("RMI registry found at " + rmiListenAddr + ":" + jndiRmiPort);
            } catch (RemoteException e) {
                LOGGER.debug("Creating RMI registry at " + rmiListenAddr + ":" + jndiRmiPort);
                RMIServerSocketFactory ssf = new PulsarJMXRMIServerSocketFactory(10, rmiListenAddr, false);
                registry = LocateRegistry.createRegistry(jndiRmiPort, null, ssf);
            }

            // Service URL
            JMXServiceURL url = new JMXServiceURL("service:jmx:rmi://" + rmiListenAddr + "/jndi/rmi://" + rmiListenAddr + ":" + jndiRmiPort + "/" + jndiName);
            LOGGER.info("JMX Service URL : " + url);

            // Create and start the RMIConnectorServer
            Map<String, Object> env = new HashMap<>();
            mBeanServerSocketFactory = new PulsarJMXRMIServerSocketFactory(10, rmiListenAddr, true);
            env.put(RMIConnectorServer.JNDI_REBIND_ATTRIBUTE, "true");
            //env.put(RMIConnectorServer.RMI_CLIENT_SOCKET_FACTORY_ATTRIBUTE, new JMXRMIClientSocketFactory(rmiListenAddr));
            env.put(RMIConnectorServer.RMI_SERVER_SOCKET_FACTORY_ATTRIBUTE, mBeanServerSocketFactory);
            connectorServer = JMXConnectorServerFactory.newJMXConnectorServer(url, env, mBeanServer);
            connectorServer.start();
        } catch (Exception e) {
            throw new PulsarJMSException("Could not initialize JMX agent", "JMX_ERROR", e);
        }
    }

    @Override
    protected String getType() {
        return PulsarConstants.JMX_RMI;
    }
}
