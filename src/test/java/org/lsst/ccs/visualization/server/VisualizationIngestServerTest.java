package org.lsst.ccs.visualization.server;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import org.lsst.ccs.visualization.client.IngestClient;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 *
 * @author tonyj
 */
public class VisualizationIngestServerTest {

    private SocketAddress address;
    private VisualizationIngestServer server;
    private TestFitsFileHandler currentHandler = new TestFitsFileHandler();

    public VisualizationIngestServerTest() {
    }

    @Before
    public void setup() throws IOException {
        FitsFileManager ffManager = new FitsFileManager(new File("/notused")) {
            @Override
            FitsFileHandler createHandler(File dir, StartMessage msg) throws IOException {
                currentHandler.init(msg);
                return currentHandler;
            }
        };
        InetSocketAddress inputAddress = new InetSocketAddress(InetAddress.getLoopbackAddress(), 0);
        server = new VisualizationIngestServer(inputAddress, ffManager);
        address = server.start();
        Thread t = new Thread(server);
        t.start();
    }

    @After
    public void teardown() throws IOException {
        server.shutdown();
    }

    @Test
    public void singleThreadTest() throws IOException, InterruptedException {
        try (IngestClient client = new IngestClient(address)) {
            StartMessage start = new StartMessage("testImage", 100, 100, 20, 1);
            client.send(start);
            ByteBuffer bb = createDummyData();
            DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
            client.send(data);
            EndMessage end = new EndMessage("testImage");
            client.send(end);
        }
        currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);
        assertEquals(100, currentHandler.getWidth());
        assertEquals(100, currentHandler.getHeight());
        assertEquals(40000, currentHandler.getnBytesReceived());
        assertEquals(0, currentHandler.getnHeadersReceived());
        assertEquals("testImage", currentHandler.getImageName());
    }

    private ByteBuffer createDummyData() {
        ByteBuffer bb = ByteBuffer.allocate(4 * 100 * 100);
        for (int i = 0; i < 100 * 100; i++) {
            bb.putInt(i);
        }
        bb.flip();
        return bb;
    }

    @Test
    public void delayedStartTest() throws IOException, InterruptedException {
        Thread t1 = new Thread() {
            @Override
            public void run() {
                try (IngestClient client = new IngestClient(address)) {
                    ByteBuffer bb = createDummyData();
                    DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
                    client.send(data);
                    EndMessage end = new EndMessage("testImage");
                    client.send(end);
                } catch (IOException x) {
                    fail(x.getMessage());
                }
            }
        };
        Thread t2 = new Thread() {
            @Override
            public void run() {
                try (IngestClient client = new IngestClient(address)) {
                    StartMessage start = new StartMessage("testImage", 100, 100, 20, 1);
                    client.send(start);
                } catch (IOException x) {
                    fail(x.getMessage());
                }
            }
        };
        t1.start();
        Thread.sleep(200);
        t2.start();
        t1.join(10000);
        t2.join(10000);

        currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);
        assertEquals(100, currentHandler.getWidth());
        assertEquals(100, currentHandler.getHeight());
        assertEquals(40000, currentHandler.getnBytesReceived());
        assertEquals(0, currentHandler.getnHeadersReceived());
        assertEquals("testImage", currentHandler.getImageName());
    }

    @Test
    public void dataWithoutStartTest() throws IOException, InterruptedException {
        try (IngestClient client = new IngestClient(address)) {
            ByteBuffer bb = createDummyData();
            DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
            client.send(data);
            StartMessage start = new StartMessage("testImage", 100, 100, 20, 1);
            client.send(start);
            EndMessage end = new EndMessage("testImage");
            client.send(end);
        }
        currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);
        assertEquals(100, currentHandler.getWidth());
        assertEquals(100, currentHandler.getHeight());
        assertEquals(0, currentHandler.getnBytesReceived());
        assertEquals(0, currentHandler.getnHeadersReceived());
        assertEquals("testImage", currentHandler.getImageName());
    }

    @Test
    public void multiThreadTest() throws IOException, InterruptedException {
        List<Thread> threads = new ArrayList<>();
        int nThreads = 9;
        for (int i = 0; i < nThreads; i++) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try (IngestClient client = new IngestClient(address)) {
                        ByteBuffer bb = createDummyData();
                        DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
                        client.send(data);
                        EndMessage end = new EndMessage("testImage");
                        client.send(end);
                    } catch (IOException x) {
                        fail(x.getMessage());
                    }
                }
            });
        }
        try (IngestClient client = new IngestClient(address)) {
            StartMessage start = new StartMessage("testImage", 100, 100, 20, nThreads);
            client.send(start);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(10000);
        }
        currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);
        assertEquals(100, currentHandler.getWidth());
        assertEquals(100, currentHandler.getHeight());
        assertEquals(nThreads * 40000, currentHandler.getnBytesReceived());
        assertEquals(0, currentHandler.getnHeadersReceived());
        assertEquals("testImage", currentHandler.getImageName());
    }

    @Test
    public void missingDataTest() throws IOException, InterruptedException {
        List<Thread> threads = new ArrayList<>();
        int nThreads = 9;
        for (int i = 0; i < nThreads; i++) {
            threads.add(new Thread() {
                @Override
                public void run() {
                    try (IngestClient client = new IngestClient(address)) {
                        ByteBuffer bb = createDummyData();
                        DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
                        client.send(data);
                        EndMessage end = new EndMessage("testImage");
                        client.send(end);
                    } catch (IOException x) {
                        fail(x.getMessage());
                    }
                }
            });
        }
        try (IngestClient client = new IngestClient(address)) {
            StartMessage start = new StartMessage("testImage", 100, 100, 20, nThreads + 1);
            client.send(start);
        }
        for (Thread t : threads) {
            t.start();
        }
        for (Thread t : threads) {
            t.join(10000);
        }
        currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);
        assertEquals(100, currentHandler.getWidth());
        assertEquals(100, currentHandler.getHeight());
        assertEquals(nThreads * 40000, currentHandler.getnBytesReceived());
        assertEquals(0, currentHandler.getnHeadersReceived());
        assertEquals("testImage", currentHandler.getImageName());
    }

    @Test
    public void shutdownWhileActiveTest() throws IOException, InterruptedException {
        try (IngestClient client = new IngestClient(address)) {
            StartMessage start = new StartMessage("testImage", 100, 100, 20, 1);
            client.send(start);
            currentHandler.waitUntilOpen(10, TimeUnit.SECONDS);
            Thread.sleep(10);
            server.shutdown();
            currentHandler.waitUntilClosed(10, TimeUnit.SECONDS);

//            try {
//                EndMessage end = new EndMessage("testImage");
//                client.send(end);
//                fail("Should have thrown exception");
//            } catch (IOException x) {
//                // OK, exception expected
//            }
        }
    }
}
