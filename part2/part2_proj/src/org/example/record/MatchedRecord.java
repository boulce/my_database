package org.example.record;

public class MatchedRecord {
    Record recordR;
    Record recordS;

    public MatchedRecord(Record recordR, Record recordS) {
        this.recordR = recordR;
        this.recordS = recordS;
    }

    public Record getRecordR() {
        return recordR;
    }

    public Record getRecordS() {
        return recordS;
    }
}
