package org.example.record;

import org.example.metadata.AttributeMetadata;
import org.example.util.ByteUtil;

import java.util.ArrayList;
import java.util.List;

import static org.example.util.ByteUtil.intToByteArray;

public class Block {
    int idx;
    Record records[];

    public int getIdx() {
        return idx;
    }

    public void setIdx(int idx) {
        this.idx = idx;
    }

    public Block(int idx, List<char[]> attChars) {
        this.idx = idx;
        records = new Record[BlockingFactor.VAL];

        Record record = new Record(attChars, 0);

        for(int i = 0; i < records.length; i++) {
            records[i] = record.copyFactory();
        }

        // Init the link. link of i-th record points (i+1)-th record
        int recordSize = record.getSize();
        int blockSize = recordSize * BlockingFactor.VAL;
        for(int i = 0; i < records.length-1; i++) {
            records[i].setLink(idx * blockSize + recordSize * (i+1));
        }
    }

    public Block(int idx, byte[] blockBytes, List<AttributeMetadata> attributeMetadataList) {
        this.idx = idx;
        this.records = new Record[BlockingFactor.VAL];

        int bytesIdx = 0;
        for(int i = 0; i < BlockingFactor.VAL; i++) {
            List<char[]> attributes = new ArrayList<>();
            for (AttributeMetadata attributeMetadata : attributeMetadataList) {
                int attributeLength = attributeMetadata.getLength();
                char[] attribute = new char[attributeLength];
                for (int j = 0; j < attributeLength; j++) {
                    attribute[j] = (char) blockBytes[bytesIdx++];
                }
                attributes.add(attribute);
            }

            byte[] linkBytes = new byte[4];
            for(int j = 0; j < 4; j++) {
                linkBytes[j] = blockBytes[bytesIdx++];
            }
            int link = ByteUtil.byteArrayToInt(linkBytes);
            records[i] = new Record(attributes, link);
        }
    }

    public Record[] getRecords() {
        return records;
    }

    public void setRecords(Record[] records) {
        this.records = records;
    }

    public byte[] getByteArray() {
        int recordSize = records[0].getSize();
        int blockSize = BlockingFactor.VAL * recordSize;
        byte[] blockBytes = new byte[blockSize]; // this bytes will be written to the file (Block I/O)

        int recordIdx = 0;
        for(int i = 0; i < records.length; i++) {
            byte[] recordBytes = new byte[recordSize];

            // Write Attribute to recordBytes
            int attIdx = 0;
            for (char[] att : records[i].getAttributes()) {
                byte[] attBytes = new byte[att.length];

                for (int j = 0; j < att.length; j++) {
                    attBytes[j] = (byte) att[j];
                }

                for (int j = 0; j < attBytes.length; j++, attIdx++) {
                    recordBytes[attIdx] = attBytes[j];
                }
            }

            int link = records[i].getLink();
            byte[] linkBytes = intToByteArray(link);
            for (int j = 0; j < linkBytes.length; j++, attIdx++) {
                recordBytes[attIdx] = linkBytes[j];
            }

            // Write record to blockBytes
            for (int j = 0; j < recordBytes.length; j++, recordIdx++) {
                blockBytes[recordIdx] = recordBytes[j];
            }
        }
        return blockBytes;
    }
}
