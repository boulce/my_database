package org.example.processor;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.example.record.NullConst.isNullAttribute;

public class QueryEvaluationEngine {
    static List<Record> getRecordsForSelectAll(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList, int recordSize, ArrayList<Integer> attPosOfPrimaryKey) throws IOException {
        List<Record> resultSet = new ArrayList<>(); // This result set will save the records to show to user

        // Read from the file
        RandomAccessFile input = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "r");
        while(true) {
            int blockSize = recordSize * BlockingFactor.VAL;

            byte[] readBlockBytes = new byte[blockSize];
            int readCnt = input.read(readBlockBytes);

            if(readCnt == -1) { // EOF
                break;
            }

            for(int i = 0; i < BlockingFactor.VAL; i++) {
                byte[] recordBytes = new byte[recordSize];
                for(int j = 0; j < recordSize; j++) {
                    recordBytes[j] = readBlockBytes[i* recordSize +j];
                }
                Record readRecord = new Record(recordBytes, attributeMetadataList);

                // If some attribute of primary-key of a record is NULL, it is a deleted record.
                // So don't contain the deleted record to the result set
                boolean isDeleted = false;
                for(int pos : attPosOfPrimaryKey) {
                    if (isNullAttribute(readRecord.getAttributes().get(pos))){
                        isDeleted = true;
                        break;
                    }
                }
                if(!isDeleted) {
                    resultSet.add(readRecord);
                }
            }
        }
        input.close();
        return resultSet;
    }
}
