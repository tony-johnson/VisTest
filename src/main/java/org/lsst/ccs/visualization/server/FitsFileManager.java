package org.lsst.ccs.visualization.server;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.Message;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 * Keeps track of open fits files.
 *
 * @author tonyj
 */
class FitsFileManager extends TimerTask {

    private final File dir;
    private BlockingMap<String, ManagedFile> handlers = new BlockingMap<>();
    private static final Logger logger = Logger.getLogger(FitsFileManager.class.getName());
    private static Timer timer = new Timer("Idle Timeout", true);
    /**
     * Create a fits file manager. This instance is shared by all incoming
     * connections.
     *
     * @param dir The directory where fits files will be created.
     */
    FitsFileManager(File dir) {
        this.dir = dir;
        timer.scheduleAtFixedRate(this, 1000, 1000);
    }

    void handleMessage(Message msg, SocketChannel socket) throws IOException {
        ManagedFile handler = getHandlerForMessage(msg);
        if (handler == null) {
            discard(msg, socket);
        } else {
            handler.fitsFileHandler.handle(msg, socket);
            handler.lastActive.set(System.currentTimeMillis());
            if (msg instanceof EndMessage) {
                int nClients = handler.nClients.decrementAndGet();
                if (nClients == 0) {
                    handlers.remove(msg.getImageName());
                    handler.fitsFileHandler.close();
                }
            }
        }
    }

    private ManagedFile getHandlerForMessage(Message msg) throws IOException {
        String imageName = msg.getImageName();
        if (msg instanceof StartMessage) {
            final StartMessage start = (StartMessage) msg;
            FitsFileHandler handler = createHandler(dir, start);
            ManagedFile file = new ManagedFile(handler, start.getnClients(), start.getImageName());
            handlers.put(imageName, file);
            return file;
        } else {
            try {
                // We wait up to 1 second, in case the start message is delayed
                return handlers.get(imageName, 1, TimeUnit.SECONDS);
            } catch (InterruptedException ex) {
                return null;
            }
        }
    }

    private void checkIdleFiles() {
        long now = System.currentTimeMillis();
        for (Map.Entry<String, ManagedFile> entry : handlers.entrySet()) {
            final ManagedFile value = entry.getValue();
            if (value.isIdle(now)) {
                logger.log(Level.WARNING, "Closing idle file {0}", value.imageName);
                handlers.remove(entry.getKey());
                try {
                    entry.getValue().fitsFileHandler.close();
                } catch (IOException ex) {
                    logger.log(Level.WARNING, "Error while closing idle file", ex);
                }
            }
        }
    }

    /**
     * Can be overridden in test cases.
     *
     * @param dir The directory where the fits files will be created.
     * @param msg The message used to create the file.
     * @return The created FitsFileHandler
     * @throws IOException If an error occurs while creating the file
     */
    FitsFileHandler createHandler(File dir, StartMessage msg) throws IOException {
        return new FitsFileHandlerImpl(dir, msg);
    }

    private void discard(Message msg, SocketChannel socket) throws IOException {
        logger.log(Level.WARNING, "Discarding message because no handler found {0}", msg);
        if (msg instanceof DataMessage) {
            discardData((DataMessage) msg, socket);
        }
    }

    static void discardData(DataMessage msg, SocketChannel socket) throws IOException {
        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(65536);
        int size = msg.getDataLength();
        while (size > 0) {
            dataBuffer.clear();
            if (size < dataBuffer.capacity()) {
                dataBuffer.limit(size);
            }
            size -= socket.read(dataBuffer);
        }
    }

    @Override
    public void run() {
        checkIdleFiles();
    }

    @Override
    public boolean cancel() {
        // Close any open files
        for (Map.Entry<String, ManagedFile> entry : handlers.entrySet()) {
            try {
                entry.getValue().fitsFileHandler.close();
            } catch (IOException ex) {
                logger.log(Level.WARNING, "Error while closing file during cancel",ex);
            }
        }
        return super.cancel();
    }
    
    

    private static class ManagedFile {

        private final String imageName;
        private final FitsFileHandler fitsFileHandler;
        private final AtomicInteger nClients;
        private final long startTime = System.currentTimeMillis();
        private final AtomicLong lastActive = new AtomicLong(startTime);

        public ManagedFile(FitsFileHandler fitsFileHandler, int nClients, String imageName) {
            this.fitsFileHandler = fitsFileHandler;
            this.nClients = new AtomicInteger(nClients);
            this.imageName = imageName;
        }

        private boolean isIdle(long now) {
            return now - lastActive.get() > 60*1000; 
        }


    }
}
