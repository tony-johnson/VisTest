package org.lsst.ccs.visualization.client;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.lsst.ccs.visualization.message.DataMessage;
import org.lsst.ccs.visualization.message.EndMessage;
import org.lsst.ccs.visualization.message.Message;
import org.lsst.ccs.visualization.message.StartMessage;

/**
 *
 * @author tonyj
 */
public class IngestClient implements Closeable {

    private final SocketChannel socket;

    public IngestClient(String hostname, int port) throws IOException  {
        this(new InetSocketAddress(hostname, port));
    }

    public IngestClient(SocketAddress address) throws IOException {
        socket = SocketChannel.open(address);
        socket.shutdownInput();
    }

    public void send(Message msg) throws IOException {
        msg.encode(socket);
    }

    @Override
    public void close() throws IOException {
        socket.close();
    }

    public static void main(String[] args) throws InterruptedException, IOException {
        try (IngestClient client = new IngestClient("localhost", 9999)) {
            StartMessage start = new StartMessage("testImage", 100, 100, 20, 1);
            client.send(start);
            ByteBuffer bb = ByteBuffer.allocate(4*100*100);
            for (int i=0; i<100*100; i++) {
                bb.putInt(i);
            }
            bb.flip();
            DataMessage data = new DataMessage("testImage", 0, 0, 0, bb);
            client.send(data);
            EndMessage end = new EndMessage("testImage");
            client.send(end);
        }
    }

}
