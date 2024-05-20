package org.example.buffer;

import org.example.record.BlockingFactor;
import org.example.record.Record;

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
}
