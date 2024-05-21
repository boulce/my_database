package org.example.record;

import org.example.metadata.AttributeMetadata;
import org.example.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

public class Record {
    List<char[]> attributes;
    Integer link;

    public Record(List<char[]> attributes, int link) {
        this.attributes = attributes;
        this.link = link;
    }

    // convert recordBytes to a Record object
    public Record(byte[] recordBytes, List<AttributeMetadata> attributeMetadataList) {
        int bytesIdx = 0;
        attributes = new ArrayList<>();
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            int attributeLength = attributeMetadata.getLength();
            char[] attribute = new char[attributeLength];
            for (int j = 0; j < attributeLength; j++) {
                attribute[j] = (char) recordBytes[bytesIdx++];
            }
            attributes.add(attribute);
        }

        byte[] linkBytes = new byte[4];
        for(int i = 0; i < 4; i++) {
            linkBytes[i] = recordBytes[bytesIdx++];
        }
        link = ByteUtil.byteArrayToInt(linkBytes);
    }

    public List<char[]> getAttributes() {
        return attributes;
    }

    public void setAttributes(List<char[]> attributes) {
        this.attributes = attributes;
    }

    public int getLink() {
        return link;
    }

    public void setLink(Integer link) {
        this.link = link;
    }

    public Record copyFactory() {
        List<char[]> copyAtt = new ArrayList<>();

        for (char[] attribute : attributes) {
            copyAtt.add(attribute.clone());
        }
        return new Record(copyAtt, link);
    }

    public int getSize() {
        int len = 0;
        for (char[] attribute : attributes) {
            len += attribute.length;
        }
        if (link != null) {
            len += 4;
        }

        return len;
    }
}
