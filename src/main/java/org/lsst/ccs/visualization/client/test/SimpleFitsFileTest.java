package org.lsst.ccs.visualization.client.test;

import org.lsst.ccs.visualization.message.HeaderMessage;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.HeaderCard;
import nom.tam.util.BufferedDataOutputStream;
import nom.tam.util.Cursor;
import org.lsst.ccs.visualization.client.IngestClient;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 *
 * @author tonyj
 */
public class SimpleFitsFileTest {

    public static void main(String[] args) throws FitsException, IOException, InterruptedException {
        String imageName = "lsst_a_890_R22_S11_C04_E000";
        Fits fits = new Fits("/home/tonyj/Data/" + imageName + ".fits");
        BasicHDU primary = fits.readHDU();
        int[] axes = primary.getAxes();
        int nHeaders = primary.getHeader().getNumberOfCards();

        try (IngestClient client = new IngestClient("localhost", 9999)) {
            StartMessage start = new StartMessage(imageName, axes[0], axes[1], nHeaders, 1);
            client.send(start);

            for (Cursor<String, HeaderCard> i = primary.getHeader().iterator(); i.hasNext();) {
                HeaderCard next = i.next();
                HeaderMessage hdr = new HeaderMessage(imageName, next.toString());
                client.send(hdr);
            }

            OutputStream out = new OutputStream() {
                private int offset = 0;

                @Override
                public void write(byte[] b, int index, int length) throws IOException {
                    DataMessage data = new DataMessage(imageName, offset, 0, 0, ByteBuffer.wrap(b,index,length));
                    client.send(data);
                    offset += b.length;
                }

                @Override
                public void write(int b) throws IOException {
                    // This will never be called
                    throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
                }

            };
            try (BufferedDataOutputStream buf = new BufferedDataOutputStream(out)) {
                primary.getData().write(buf);
            }
            EndMessage end = new EndMessage(imageName);
            client.send(end);
        }
    }
}
