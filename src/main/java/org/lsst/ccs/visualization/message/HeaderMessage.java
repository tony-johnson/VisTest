package org.lsst.ccs.visualization.message;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 *
 * @author tonyj
 */
public class HeaderMessage extends Message {

    private final String card;

    public HeaderMessage(String imageName, String card) {
        super(MessageType.HEADER, card.length(), imageName);
        this.card = card;
    }

    @Override
    void fill(ByteBuffer bb) {
        bb.put(card.getBytes(StandardCharsets.US_ASCII));
    }


    static Message decode(ByteBuffer bb, CharSequence name) {
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        String card = new String(bytes, StandardCharsets.US_ASCII);
        return new HeaderMessage(name.toString(), card);
    }

    public String getCard() {
        return card;
    }

    @Override
    public String toString() {
        return "HeaderMessage{" + "imageName=" + getImageName() + ", card=" + card + '}';
    }

}
