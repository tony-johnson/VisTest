package org.lsst.ccs.visualization.message;

import java.nio.ByteBuffer;

/**
 * Message received to indicate a new image is going to be sent.
 *
 * @author tonyj
 */
public class StartMessage extends Message {

    private final int width;
    private final int height;
    private final int nHeaders;
    private final int nClients;

    public StartMessage(String imageName, int width, int height, int nHeaders, int nClients) {
        super(Message.MessageType.START, 16, imageName);
        this.width = width;
        this.height = height;
        this.nHeaders = nHeaders;
        this.nClients = nClients;
    }

    public int getWidth() {
        return width;
    }

    public int getHeight() {
        return height;
    }

    public int getnHeaders() {
        return nHeaders;
    }

    public int getnClients() {
        return nClients;
    }

    @Override
    void fill(ByteBuffer bb) {
        bb.putInt(width);
        bb.putInt(height);
        bb.putInt(nHeaders);
        bb.putInt(nClients);
    }
    
    static Message decode(ByteBuffer bb, CharSequence name) {
        int width = bb.getInt();
        int height = bb.getInt();
        int nHeaders = bb.getInt();
        int nClients = bb.getInt();
        return new StartMessage(name.toString(), width, height, nHeaders, nClients);
    }

    @Override
    public String toString() {
        return "StartMessage{" + "imageName=" + getImageName() + ", width=" + width + ", height=" + height + ", nHeaders=" + nHeaders + ", nClients=" + nClients+ '}';
    }

}
