package org.example.buffer;

import org.example.record.BlockingFactor;
import org.example.record.Record;

import static org.example.util.ByteUtil.intToByteArray;

public class BufferPage {
    Record[] records;
    int recordCnt;

    public BufferPage() {
        recordCnt = 0;
        this.records = new Record[BlockingFactor.VAL];
    }

    public Record[] getRecords() {
        return records;
    }

    public int getRecordCnt() {
        return recordCnt;
    }

    public void addRecord(Record record) {
        record.setLink(null); // This record will be saved into the partition file of which data structure is basic fixed-length file structure that doesn't contain link field.
                              // So just the link is useless and make link null.
        records[recordCnt++] = record;
    }

    public boolean isFull() {
        if(recordCnt == records.length) {
            return true;
        }

        return false;
    }

    public void clear() {
        recordCnt = 0;
    }

    public byte[] getByteArray() {
        int recordSize = records[0].getSize();
        int blockSize = BlockingFactor.VAL * recordSize;
        byte[] blockBytes = new byte[blockSize]; // this bytes will be written to the file (Block I/O)

        int recordIdx = 0;
        for(int i = 0; i < recordCnt; i++) {
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

            // Write record to blockBytes
            for (int j = 0; j < recordBytes.length; j++, recordIdx++) {
                blockBytes[recordIdx] = recordBytes[j];
            }
        }
        return blockBytes;
    }
}
