package com.echostreams.pulsar.jms.jmx.rmi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ServerSocketFactory;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.rmi.server.RMIServerSocketFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

public final class PulsarJMXRMIServerSocketFactory implements RMIServerSocketFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(PulsarJMXRMIServerSocketFactory.class);

    // Attributes
    private String listenAddr;
    private int backLog;
    private boolean manageSockets;

    // Cache
    private InetAddress listenIf;
    private ServerSocketFactory socketFactory;

    // Runtime
    private List<ServerSocket> createdSockets = new Vector<>();

    /**
     * Constructor
     */
    public PulsarJMXRMIServerSocketFactory(int backLog, String listenAddr, boolean manageSockets) {
        this.backLog = backLog;
        this.listenAddr = listenAddr;
        this.manageSockets = manageSockets;
    }

    private synchronized ServerSocketFactory getSocketFactory() {
        if (socketFactory == null)
            socketFactory = ServerSocketFactory.getDefault();

        return socketFactory;
    }

    private synchronized InetAddress getListenAddress() throws UnknownHostException {
        if (listenIf == null) {
            // Resolve listen interface
            listenIf = InetAddress.getByName(listenAddr);
        }
        return listenIf;
    }

    @Override
    public ServerSocket createServerSocket(int port) throws IOException {
        ServerSocket socket = getSocketFactory().createServerSocket(port, backLog, getListenAddress());
        if (manageSockets)
            createdSockets.add(socket);
        return socket;
    }

    /**
     * Cleanup sockets created by this factory
     */
    public void close() {
        if (!manageSockets)
            throw new IllegalStateException("Cannot close an un-managed socket factory");

        synchronized (createdSockets) {
            Iterator<ServerSocket> sockets = createdSockets.iterator();
            while (sockets.hasNext()) {
                ServerSocket socket = sockets.next();
                try {
                    socket.close();
                } catch (Exception e) {
                    LOGGER.error("Could not close server socket", e);
                }
            }
            createdSockets.clear();
        }
    }
}
