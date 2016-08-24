package org.lsst.ccs.visualization.server;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The main class for the ingest server.
 *
 * @author tonyj
 */
public class VisualizationIngestServer implements Runnable {

    private final SocketAddress address;
    private final FitsFileManager ffManager;
    private static final Logger LOGGER = Logger.getLogger(VisualizationIngestServer.class.getName());
    private ServerSocketChannel server;
    private Set<MessageHandler> weakMessageSet = Collections.newSetFromMap(new WeakHashMap<MessageHandler,Boolean>());

    /**
     * Create the ingest server
     *
     * @param port The port on which the ingest server will listen
     * @param dir The directory where received file will be placed. It is highly
     * recommended that this be on SSD disk or
     * <a href="https://www.jamescoyle.net/how-to/943-create-a-ram-disk-in-linux">ramdisk</a>
     * since otherwise performance will be limited by the disk
     * <a href="https://en.wikipedia.org/wiki/IOPS">iops</a>.
     */
    public VisualizationIngestServer(int port, File dir) {
        this(new InetSocketAddress(port), new FitsFileManager(dir));
    }

    VisualizationIngestServer(SocketAddress address, FitsFileManager ffManager) {
        this.address = address;
        this.ffManager = ffManager;
    }

    /**
     * Run the server. This method will listen for incoming connections, and not
     * return until shutdown is called, or a fatal error occurs.
     */
    @Override
    public void run() {
        try {
            if (server == null || !server.isOpen()) start();
            for (;;) {
                SocketChannel accept = server.accept();
                LOGGER.log(Level.INFO, "Accepted incoming connection from {0}", accept.getRemoteAddress());
                // TODO: Currently uses one thread per socket. Could be modified to use a thread pool and
                // selectors later if neccessary (may never be).
                MessageHandler handler = new MessageHandler(accept, ffManager);
                weakMessageSet.add(handler);
                handler.start();
            }
        } catch (ClosedChannelException x) {
            // OK, presumably cause by shutdown request.
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "Uncaught IOException while handling sockets", ex);
        }
    }

    /**
     * Starts the server. Return once the server has started listening.
     */
    SocketAddress start() throws IOException {
        server = ServerSocketChannel.open();
        server.bind(address);
        LOGGER.log(Level.INFO, "Listening for incoming connection on {0}", server.getLocalAddress());
        return server.getLocalAddress();
    }

    public void shutdown() throws IOException {
        for (MessageHandler messageHandler : weakMessageSet) {
            messageHandler.close();
        }
        ffManager.cancel();
        server.close();
    }

    public static void main(String[] args) throws Exception {
        int port;
        if (args.length > 0) {
            port = Integer.parseInt(args[0]);
        } else {
            port = 9999;
        }
        new VisualizationIngestServer(port, new File("/tmp")).run();
    }
}
