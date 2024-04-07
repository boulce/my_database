package org.example.processor;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.Block;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.example.processor.DMLOrganizer.getRecordSize;
import static org.example.record.NullConst.NULL_LINK;
import static org.example.record.NullConst.isNullAttribute;

public class QueryEvaluationEngine {
    List<Record> getRecordsForSelectAll(RelationMetadata relationMetadata, List<AttributeMetadata> attributeMetadataList, int recordSize, List<Integer> attPosOfPrimaryKey) throws IOException {
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

    List<Record> getRecordsForSelectOne(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList, int recordSize, Map<Integer, String> primaryKeyMap) throws IOException {
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
                for(int pos : primaryKeyMap.keySet()) {
                    if (isNullAttribute(readRecord.getAttributes().get(pos))){
                        isDeleted = true;
                        break;
                    }
                }
                if(!isDeleted) {
                    boolean found = true;
                    for (Integer key : primaryKeyMap.keySet()) {
                        if(!new String(readRecord.getAttributes().get(key)).trim().equals(primaryKeyMap.get(key))) { // compare search key and read record attribute
                            found = false;
                            break;
                        }
                    }
                    if(found) {
                        resultSet.add(readRecord);
                        break;
                    }
                }
            }
        }
        input.close();
        return resultSet;
    }

    static void processInsertQuery(RelationMetadata relationMetadata, Record recordToInsert, ArrayList<AttributeMetadata> attributeMetadataList) throws IOException {
        // Read the header block in the file
        RandomAccessFile file = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "rw");
        int recordSize = recordToInsert.getSize();
        int blockSize = recordSize * BlockingFactor.VAL;
        byte[] readBlockBytes = new byte[blockSize];
        file.read(readBlockBytes);

        Block headerBlock = new Block(0, readBlockBytes, attributeMetadataList);

        // Get header link
        int headerLink = headerBlock.getRecords()[0].getLink();

        if(headerLink == NULL_LINK) {
            Block newBlock = new Block((int) (file.length() / blockSize), DDLInterpreter.getAttChars(attributeMetadataList));

            // Assign link of target record to header's link
            headerBlock.getRecords()[0].setLink(newBlock.getRecords()[0].getLink());
            newBlock.getRecords()[0] = recordToInsert;
            // Write the header block and new block that contains the record to be inserted to the file
            file.seek(0);
            file.write(headerBlock.getByteArray());

            file.seek(file.length());
            file.write(newBlock.getByteArray());
        } else {
            // Find the block of the record to which insert a new record
            int nextBlockIdx = headerLink / blockSize; // Block Idx that header points
            if(nextBlockIdx == headerBlock.getIdx()) { // Next record that header point is in header block
                int recordOffset = headerLink;
                int recordIdx = recordOffset / recordSize;
                // Assign link of target record to header's link
                headerBlock.getRecords()[0].setLink(headerBlock.getRecords()[recordIdx].getLink());

                // Insert a new record to target record position
                headerBlock.getRecords()[recordIdx] = recordToInsert;

                // Write header block to the file
                file.seek(headerBlock.getIdx() * blockSize);
                file.write(headerBlock.getByteArray());
                file.close();
            } else { // Next record that header point isn' in header block
                int recordOffset = headerLink - nextBlockIdx*blockSize;
                int recordIdx = recordOffset / recordSize;

                // Read the block to which insert a new block
                file.seek(nextBlockIdx*blockSize);
                file.read(readBlockBytes);

                Block targetBlock = new Block(nextBlockIdx, readBlockBytes, attributeMetadataList);

                // Assign link of target record to header's link
                headerBlock.getRecords()[0].setLink(targetBlock.getRecords()[recordIdx].getLink());

                // Insert a new record to target record position
                targetBlock.getRecords()[recordIdx] = recordToInsert;

                // Write header block to the file
                file.seek(headerBlock.getIdx() * blockSize);
                file.write(headerBlock.getByteArray());

                // Write target block to the file
                file.seek(targetBlock.getIdx() * blockSize);
                file.write(targetBlock.getByteArray());

                file.close();
            }
            file.close();
        }
    }

    static void processDeleteQuery(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList, HashMap<Integer, String> primaryKeyMap) throws IOException {
        // Read from the file
        List<Record> resultSet = new ArrayList<>();
        RandomAccessFile file = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "rw");

        int recordSize = getRecordSize(attributeMetadataList);
        int blockSize = recordSize * BlockingFactor.VAL;

        byte[] headerBlockBytes = new byte[blockSize];
        file.read(headerBlockBytes);
        Block headerBlock = new Block(0, headerBlockBytes, attributeMetadataList);

        file.seek(0);
        Block readBlock = null;
        int recordIdx = 0; // Record Index to be deleted
        for(int blockIdx = 0; ; blockIdx++){

            byte[] readBlockBytes = new byte[blockSize];
            int readCnt = file.read(readBlockBytes);

            if(readCnt == -1) { // EOF
                break;
            }

            readBlock = new Block(blockIdx, readBlockBytes, attributeMetadataList);

            for(recordIdx = 0; recordIdx < BlockingFactor.VAL; recordIdx++) {
                Record readRecord = readBlock.getRecords()[recordIdx];

                // If some attribute of primary-key of a record is NULL, it is a deleted record.
                // So don't contain the deleted record to the result set
                boolean isDeleted = false;
                for(int pos : primaryKeyMap.keySet()) {
                    if (isNullAttribute(readRecord.getAttributes().get(pos))){
                        isDeleted = true;
                        break;
                    }
                }
                if(!isDeleted) {
                    boolean found = true;
                    for (Integer key : primaryKeyMap.keySet()) {
                        if(!new String(readRecord.getAttributes().get(key)).trim().equals(primaryKeyMap.get(key))) { // compare search key and read record attribute
                            found = false;
                            break;
                        }
                    }
                    if(found) { // Found the block to be deleted
                        resultSet.add(readRecord);
                        break;
                    }
                }
            }

            if(!resultSet.isEmpty()){ // Found the block to be deleted
                break;
            }
        }

        if(!resultSet.isEmpty()){ // Found the block to be deleted

            Record recordToDelete = resultSet.get(0);
            // Make the record to be deleted the null record
            List<char[]> nullAtts = recordToDelete.getAttributes().stream().map(att -> new char[att.length]).collect(Collectors.toList());

            if(headerBlock.getIdx() == readBlock.getIdx()) { // If block that contains the record to be deleted is same to header block
                recordToDelete.setAttributes(nullAtts);
                recordToDelete.setLink(0);

                Record headerRecord = readBlock.getRecords()[0];
                // Set the link of record to be deleted as the link of header
                recordToDelete.setLink(headerRecord.getLink());

                // Set the link of header as byte offset of record to be deleted
                headerRecord.setLink(readBlock.getIdx()*blockSize + recordIdx*recordSize);

                file.seek(readBlock.getIdx()*blockSize);
                file.write(readBlock.getByteArray());
            } else { // If block that contains the record to be deleted is not same to header block
                recordToDelete.setAttributes(nullAtts);
                recordToDelete.setLink(0);

                Record headerRecord = headerBlock.getRecords()[0];
                // Set the link of record to be deleted as the link of header
                recordToDelete.setLink(headerRecord.getLink());

                // Set the link of header as byte offset of record to be deleted
                headerRecord.setLink(readBlock.getIdx()*blockSize + recordIdx*recordSize);

                file.seek(headerBlock.getIdx()*blockSize);
                file.write(headerBlock.getByteArray());

                file.seek(readBlock.getIdx()*blockSize);
                file.write(readBlock.getByteArray());
            }

        } else { // Not found the block to be deleted
            System.out.println("[ERROR] There isn't the record that has the primary key you entered.");
            System.out.println();
        }

        file.close();
    }
}
