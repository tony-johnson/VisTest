package org.lsst.ccs.visualization.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lsst.ccs.visualization.message.Message;

/**
 * Reads messages from an open socket, and dispatches them to the FitsFileManager.
 * @author tonyj
 */
class MessageHandler extends Thread {

    private final ByteBuffer headerBuffer = ByteBuffer.allocateDirect(256);
    private final SocketChannel socket;
    private final FitsFileManager ffManager;
    private static final Logger LOGGER = Logger.getLogger(MessageHandler.class.getName());
    

    MessageHandler(SocketChannel accept, FitsFileManager ffManager) throws IOException {
        this.ffManager = ffManager;
        this.socket = accept;
        accept.shutdownOutput();
    }

    @Override
    public void run() {
        try {
            for (;;) {
                Message message = readMessage();
                if (message == null) {
                    break;
                }
                LOGGER.log(Level.FINE, "Received: {0}", message);
                ffManager.handleMessage(message, socket);
            }
        } catch (ClosedChannelException x) {
            // OK, just means socket has been closed down
        } catch (IOException ex) {
            LOGGER.log(Level.SEVERE, "IOException handling message on socket "+socket, ex);
        } finally {
            try {
                LOGGER.log(Level.FINE, "Closing connection to : {0}", socket);
                socket.close();
            } catch (IOException ex) {
                LOGGER.log(Level.WARNING, "IOException closing socket "+socket, ex);
            }
        }
    }

    private Message readMessage() throws IOException {
        headerBuffer.clear();
        headerBuffer.limit(4);
        int l = socket.read(headerBuffer);
        if (l < 0) {
            return null;
        }
        int len = headerBuffer.getInt(0);
        headerBuffer.limit(len + 4);
        socket.read(headerBuffer);
        headerBuffer.flip();
        return Message.decode(headerBuffer);
    }

    void close() throws IOException {
        socket.close();
    }
}
