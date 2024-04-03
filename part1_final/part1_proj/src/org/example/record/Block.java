package org.example.record;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Block {
    int idx;
    Record records[];

    public Block(int idx, List<char[]> attChars) {
        this.idx = idx;
        records = new Record[BlockingFactor.VAL];

        Record nullRecord = new Record(attChars, 0);

        for(int i = 0; i < records.length; i++) {
            records[i] = nullRecord.copyFactory();
        }

        // Init the link. link of i-th record points (i+1)-th record
        int recordSize = nullRecord.getSize();
        int blockSize = recordSize * BlockingFactor.VAL;
        for(int i = 0; i < records.length-1; i++) {
            records[i].setLink(idx * blockSize + recordSize * (i+1));
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

    private  byte[] intToByteArray(int value) {
        byte[] byteArray = new byte[4];
        byteArray[0] = (byte)(value >> 24);
        byteArray[1] = (byte)(value >> 16);
        byteArray[2] = (byte)(value >> 8);
        byteArray[3] = (byte)(value);
        return byteArray;
    }

    private  int byteArrayToInt(byte bytes[]) {
        return ((((int)bytes[0] & 0xff) << 24) |
                (((int)bytes[1] & 0xff) << 16) |
                (((int)bytes[2] & 0xff) << 8) |
                (((int)bytes[3] & 0xff)));
    }
}
