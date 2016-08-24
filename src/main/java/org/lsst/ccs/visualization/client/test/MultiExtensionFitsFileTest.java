package org.lsst.ccs.visualization.client.test;

import java.io.File;
import java.io.IOException;
import nom.tam.fits.FitsException;
import org.lsst.ccs.visualization.client.IngestClient;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 *
 * @author tonyj
 */
public class MultiExtensionFitsFileTest {

    public static void main(String[] args) throws FitsException, IOException, InterruptedException {
        String imageName = "snap_1440463613702-firstset-dark-500-0";
        File fits = new File("/home/tonyj/Data/" + imageName + ".fits");
        MultiExtensionFitsFile ff = new MultiExtensionFitsFile(fits);
        long startMillis = System.currentTimeMillis();
        // Now send the image 
        try (IngestClient client = new IngestClient("localhost", 9999)) {
            for (int n = 0; n < 10; n++) {
                StartMessage start = new StartMessage(imageName, ff.getWidth(), ff.getHeight(), ff.getNHeaders(), 1);
                client.send(start);
                ff.sendHeaders(client, imageName);
                ff.sendData(client, imageName, 0, 0, 0);
                EndMessage end = new EndMessage(imageName);
                client.send(end);
            }
        }
        long stopMillis = System.currentTimeMillis();
        System.out.printf("Send took %,dms\n", stopMillis - startMillis);
    }

}
