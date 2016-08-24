package org.lsst.ccs.visualization.client.test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;
import nom.tam.fits.FitsException;
import org.lsst.ccs.visualization.client.IngestClient;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 *
 * @author tonyj
 */
public class RaftLevelFitsFileTest {

    public static void main(String[] args) throws FitsException, IOException, InterruptedException, ExecutionException, BrokenBarrierException {
        File dir = new File("/home/tonyj/Data/raft");
        String imageName = "r99_fe55_test_000_20160628175107";
        List<CCDThread> threads = new ArrayList<>();

        Runnable barrierAction = new Runnable() {
            private int ccdHeight;
            private int ccdWidth;
            private boolean first = true;
            private IngestClient client = new IngestClient("localhost",9999);
            private int raftWidth;
            private int raftHeight;
            private long begin = System.currentTimeMillis();

            @Override
            public void run() {
                try {
                    if (first) {

                        long now = System.currentTimeMillis();
                        System.out.printf("Loading files took %,dms\n", now - begin);
                        CCDThread thread = threads.get(0);

                        ccdWidth = thread.getWidth();
                        ccdHeight = thread.getHeight();
                        raftWidth = 20 + 3 * ccdWidth;
                        raftHeight = 20 + 3 * ccdHeight;
                        System.out.printf("raft %d %d %,d\n", raftWidth, raftHeight, 4 * raftWidth * raftHeight);
                        for (CCDThread t : threads) {
                            t.setData("localhost", 9999, raftWidth);

                        }
                        first = false;
                    } else {
                        long now = System.currentTimeMillis();
                        System.out.printf("Sending files took %,dms\n", now - begin);
                    }
                    Thread.sleep(10000);
                    begin = System.currentTimeMillis();
                    StartMessage start = new StartMessage(imageName, raftWidth, raftHeight, 10, 9);
                    client.send(start);
                } catch (IOException | InterruptedException ex) {
                    Logger.getLogger(RaftLevelFitsFileTest.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
        };
        CyclicBarrier barrier = new CyclicBarrier(9, barrierAction);
        // We create one thread per CCD
        for (int x = 0; x < 3; x++) {
            for (int y = 0; y < 3; y++) {
                CCDThread thread = new CCDThread(dir, imageName, barrier, x, y);
                threads.add(thread);
                thread.start();
            }
        }
    }

    private static class CCDThread extends Thread {

        private final File dir;
        private final String imageName;
        private final CyclicBarrier barrier;
        private final int x;
        private final int y;
        private MultiExtensionFitsFile fitsFile;
        private IngestClient client;
        private int raftWidth;

        private CCDThread(File dir, String imageName, CyclicBarrier barrier, int x, int y) {
            this.dir = dir;
            this.imageName = imageName;
            this.barrier = barrier;
            this.x = x;
            this.y = y;
        }

        private void setData(String hostname, int port, int raftWidth) throws IOException {
            this.client = new IngestClient(hostname, port);
            this.raftWidth = raftWidth;
        }

        @Override
        public void run() {
            try {
                String filename = String.format("s%01d%01d_" + imageName, y, x);
                File file = new File(dir, filename + ".fits");
                fitsFile = new MultiExtensionFitsFile(file);
                // Wait for everyone to get here
                barrier.await();
                // setData will have been called by now
                int offset = x * (10 + getWidth()) + y * (10 + getHeight()) * raftWidth;
                for (;;) {
                    // send file fits file, and wait again
                    fitsFile.sendData(client, imageName, offset, getWidth(), raftWidth);
                    barrier.await();
                }

            } catch (FitsException | IOException | InterruptedException | BrokenBarrierException ex) {
                Logger.getLogger(RaftLevelFitsFileTest.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        private int getWidth() {
            return fitsFile.getWidth();
        }

        private int getHeight() {
            return fitsFile.getHeight();
        }

    }

}
