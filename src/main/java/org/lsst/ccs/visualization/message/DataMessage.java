package org.lsst.ccs.visualization.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 *
 * @author tonyj
 */
public class DataMessage extends Message {

    private final int offset;
    private final int stepLength;
    private final int stepOffset;
    private final int dataLength;
    private ByteBuffer data;

    public DataMessage(String imageName, int offset, int stepLength, int stepOffset, ByteBuffer data) {
        this(imageName, offset, stepLength, stepOffset, data.remaining());
        this.data = data;
    }

    private DataMessage(String imageName, int offset, int stepLength, int stepOffset, int dataLength) {
        super(Message.MessageType.DATA, 16, imageName);
        this.offset = offset;
        this.stepLength = stepLength;
        this.stepOffset = stepOffset;
        this.dataLength = dataLength;
    }

    public int getOffset() {
        return offset;
    }

    public int getStepLength() {
        return stepLength;
    }

    public int getStepOffset() {
        return stepOffset;
    }

    public int getDataLength() {
        return dataLength;
    }

    @Override
    void fill(ByteBuffer bb) {
        bb.putInt(offset);
        bb.putInt(stepLength);
        bb.putInt(stepOffset);
        bb.putInt(dataLength);
    }

    @Override
    public void encode(SocketChannel out) throws IOException {
        super.encode(out);
        out.write(data);
    }

    static Message decode(ByteBuffer bb, CharSequence name) {
        int offset = bb.getInt();
        int stepLength = bb.getInt();
        int stepOffset = bb.getInt();
        int dataLength = bb.getInt();
        return new DataMessage(name.toString(), offset, stepLength, stepOffset, dataLength);
    }

    @Override
    public String toString() {
        return "DataMessage{" + "imageName=" + getImageName() + ", offset=" + offset + ", stepLength=" + stepLength + ", stepOffset=" + stepOffset + ", dataLength=" + dataLength + '}';
    }

}
