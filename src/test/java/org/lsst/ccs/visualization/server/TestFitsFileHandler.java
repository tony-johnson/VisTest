package org.lsst.ccs.visualization.server;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.Message;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 * A dummy implementation of FitsFileHandler which does not create any fits files, 
 * but keeps track of various counts for testing purposes.
 * @author tonyj
 */
class TestFitsFileHandler implements FitsFileHandler {

    private int width;
    private int height;
    private int nHeaders;
    private String imageName;
    private final AtomicInteger nCloses = new AtomicInteger();
    private final AtomicInteger nHeadersReceived = new AtomicInteger();
    private final AtomicLong nBytesReceived = new AtomicLong();
    private CountDownLatch openLatch = new CountDownLatch(1);
    private CountDownLatch closedLatch = new CountDownLatch(1);
    
    void init(StartMessage msg) {
        width = msg.getWidth();
        height = msg.getHeight();
        nHeaders = msg.getnHeaders();
        imageName = msg.getImageName();
        openLatch.countDown();
    }

    @Override
    public void handle(Message msg, SocketChannel socket) throws IOException {
        switch (msg.getType()) {            
            case HEADER: 
                nHeadersReceived.incrementAndGet();
                break;
                
            case DATA:
                DataMessage data = (DataMessage) msg;
                nBytesReceived.addAndGet(data.getDataLength());
                FitsFileManager.discardData(data, socket);
                break;
        }
    }

    @Override
    public void close() throws IOException {
        nCloses.incrementAndGet();
        closedLatch.countDown();
    }

    void waitUntilClosed(int time, TimeUnit unit) throws InterruptedException {
        boolean await = closedLatch.await(time, unit);
        if (!await) throw new RuntimeException("timeout");
    }

    void waitUntilOpen(int time, TimeUnit unit) throws InterruptedException {
        boolean await = openLatch.await(time, unit);
        if (!await) throw new RuntimeException("timeout");
    }
    
    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public String getImageName() {
        return imageName;
    }

    public int getnHeadersReceived() {
        return nHeadersReceived.get();
    }

    public long getnBytesReceived() {
        return nBytesReceived.get();
    }
    
}
