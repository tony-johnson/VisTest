package org.lsst.ccs.visualization.message;

import java.nio.ByteBuffer;


/**
 * Message sent to indicate that image sending is complete.
 * @author tonyj
 */
public class EndMessage extends Message {

    public EndMessage(String imageName) {
        super(Message.MessageType.END, 0, imageName);
    }

    static Message decode(ByteBuffer bb, CharSequence name) {
        return new EndMessage(name.toString());
    }

    @Override
    public String toString() {
        return "EndMessage{" + "imageName=" + getImageName() + '}';
    }
    
}
