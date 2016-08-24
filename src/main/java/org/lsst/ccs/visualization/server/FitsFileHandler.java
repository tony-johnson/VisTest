package org.lsst.ccs.visualization.server;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.SocketChannel;
import org.lsst.ccs.visualization.message.Message;

/**
 * An interface to be implemented by FitsFileHndler. Useful for testing. 
 * @author tonyj
 */
interface FitsFileHandler extends Closeable {

    void handle(Message msg, SocketChannel socket) throws IOException;
    
}
