package org.lsst.ccs.visualization.message;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;

/**
 * Base class for all messages.
 *
 * @author tonyj
 */
public class Message {

    private final int headerLength;

    public enum MessageType {
        START, END, DATA, HEADER
    }
    private final MessageType type;
    private final String imageName;

    public Message(MessageType type, int length, String imageName) {
        this.type = type;
        this.imageName = imageName;
        this.headerLength = length + 2 + imageName.length();
    }

    public String getImageName() {
        return imageName;
    }

    public MessageType getType() {
        return type;
    }

    public void encode(SocketChannel socket) throws IOException {
        ByteBuffer bb = ByteBuffer.allocate(headerLength+4);
        bb.putInt(headerLength);
        bb.put((byte) type.ordinal());
        bb.put((byte) imageName.length());
        bb.put(imageName.getBytes(StandardCharsets.US_ASCII));
        fill(bb);
        bb.flip();
        socket.write(bb);
    }

    void fill(ByteBuffer bb) {
        // NOOP by default
    }
    
    public static Message decode(ByteBuffer bb) throws IOException {
        int len = bb.getInt();
        MessageType type = MessageType.values()[bb.get()];
        int nameLength = bb.get();
        byte[] bytes = new byte[nameLength];
        bb.get(bytes);
        String name = new String(bytes,StandardCharsets.US_ASCII);
        switch (type) {
            case START:
                return StartMessage.decode(bb, name);
            case END:
                return EndMessage.decode(bb, name);
            case DATA:
                return DataMessage.decode(bb, name);
            case HEADER:
                return HeaderMessage.decode(bb, name);
            default:
                throw new IOException("Unknown message type" + type);
        }
    }
}
