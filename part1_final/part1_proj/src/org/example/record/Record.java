package org.example.record;

import java.util.ArrayList;
import java.util.List;

public class Record {
    List<char[]> attributes;
    int link;

    public Record(List<char[]> attributes, int link) {
        this.attributes = attributes;
        this.link = link;
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

    public void setLink(int link) {
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
        len += 4;

        return len;
    }
}
