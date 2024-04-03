package org.example.record;

import java.util.Arrays;
import java.util.List;

public class Block {
    Record records[];

    public Block(List<char[]> attChars) {
        records = new Record[BlockingFactor.VAL];

        Record nullRecord = new Record(attChars, 0);

        for(int i = 0; i < records.length; i++) {
            records[i] = nullRecord.copyFactory();
        }
    }



    public Record[] getRecords() {
        return records;
    }

    public void setRecords(Record[] records) {
        this.records = records;
    }
}
