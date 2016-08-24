package org.lsst.ccs.visualization.client.test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import nom.tam.fits.BasicHDU;
import nom.tam.fits.Fits;
import nom.tam.fits.FitsException;
import nom.tam.fits.HeaderCard;
import nom.tam.fits.header.Standard;
import nom.tam.util.Cursor;
import org.lsst.ccs.visualization.client.IngestClient;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.HeaderMessage;

/**
 *
 * @author tonyj
 */
public class MultiExtensionFitsFile {

    private static final Pattern headerPattern = Pattern.compile("\\[(\\d+):(\\d+),(\\d+):(\\d+)\\]");
    private final ByteBuffer bb;
    private final int nHeaders;
    private final int[] detsize;
    private final List<String> headers = new ArrayList<>();

    MultiExtensionFitsFile(File file) throws FitsException, IOException {
        try (Fits fits = new Fits(file)) {
            BasicHDU primary = fits.readHDU();
            detsize = parseIRAFHeader("DETSIZE", primary);
            System.out.printf("xsize=%d ysize=%d\n", detsize[1], detsize[3]);
            // Bug fix for old file
            //detsize[1] = 4072;
            //detsize[3] = 4000;
            // Allocate memory for image, assuming 4 bytes per pixel
            // Note, this must be allocateDirect to avoid a copy inside netty.
            bb = ByteBuffer.allocateDirect(4 * detsize[1] * detsize[3]);
            bb.limit(4 * detsize[1] * detsize[3]);
            IntBuffer intBuffer = bb.asIntBuffer();
            // Now loop over image extensions, copy into the byte buffer, after cutting off pre-post scan
            for (int n = 1; n <= 16; n++) {
                BasicHDU ext = fits.readHDU();
                int[] detsec = parseIRAFHeader("DETSEC", ext);
                int[] datasec = parseIRAFHeader("DATASEC", ext);
                Object rdata = ext.getData().getData();
                if (rdata instanceof short[][]) {
                    short[][] data = (short[][]) rdata;
                    System.out.printf("%s %s %d %d\n", Arrays.toString(detsec), Arrays.toString(datasec), data.length, data[0].length);
                    for (int x = datasec[0] - 1, xx = detsec[0] - 1; x < datasec[1]; x++, xx += detsec[1] - detsec[0] > 0 ? +1 : -1) {
                        for (int y = datasec[2] - 1, yy = detsec[2] - 1; y < datasec[3]; y++, yy += detsec[3] - detsec[2] > 0 ? +1 : -1) {
                            intBuffer.put(xx + yy * detsize[1], data[y][x]);
                        }
                    }
                } else if (rdata instanceof int[][]) {
                    int[][] data = (int[][]) rdata;
                    System.out.printf("%s %s %d %d\n", Arrays.toString(detsec), Arrays.toString(datasec), data.length, data[0].length);
                    for (int x = datasec[0] - 1, xx = detsec[0] - 1; x < datasec[1]; x++, xx += detsec[1] - detsec[0] > 0 ? +1 : -1) {
                        for (int y = datasec[2] - 1, yy = detsec[2] - 1; y < datasec[3]; y++, yy += detsec[3] - detsec[2] > 0 ? +1 : -1) {
                            intBuffer.put(xx + yy * detsize[1], data[y][x]);
                        }
                    }
                } else {
                    throw new IOException("Unknown image data type");
                }
            }
            System.out.printf("bb.size=%d\n", bb.remaining());
            primary.addValue(Standard.BITPIX, 32);
            primary.addValue(Standard.NAXIS, 2);
            nHeaders = primary.getHeader().getNumberOfCards();
            for (Cursor<String, HeaderCard> i = primary.getHeader().iterator(); i.hasNext();) {
                HeaderCard next = i.next();
                headers.add(next.toString());
            }
        }
    }

    int getWidth() {
        return detsize[1];
    }

    int getHeight() {
        return detsize[3];
    }

    int getNHeaders() {
        return nHeaders;
    }

    void sendHeaders(IngestClient client, String imageName) throws IOException {
        for (String header : headers) {
            HeaderMessage hdr = new HeaderMessage(imageName, header);
            client.send(hdr);
        }
    }

    void sendData(IngestClient client, String imageName, int offset, int stepLength, int stepOffset) throws IOException {
        DataMessage data = new DataMessage(imageName, offset, stepLength, stepOffset, bb.asReadOnlyBuffer());
        client.send(data);
        EndMessage end = new EndMessage(imageName);
        client.send(end);
    }

    private int[] parseIRAFHeader(String name, BasicHDU hdu) throws IOException {
        String header = hdu.getHeader().getStringValue(name);
        if (header == null) {
            //throw new IOException("Missing " + name + " keyword in header " + hdu);
            header = "[1:4096,1:4004]";
        }
        Matcher matcher = headerPattern.matcher(header);
        if (!matcher.matches()) {
            throw new IOException("Invalid " + name + " keyword in header " + hdu + ": " + header);
        }
        int x1 = Integer.parseInt(matcher.group(1));
        int x2 = Integer.parseInt(matcher.group(2));
        int y1 = Integer.parseInt(matcher.group(3));
        int y2 = Integer.parseInt(matcher.group(4));
        return new int[]{x1, x2, y1, y2};
    }
}
