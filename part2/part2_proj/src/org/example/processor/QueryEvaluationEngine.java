package org.example.processor;

import org.example.buffer.BufferPage;
import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.*;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.example.buffer.JoinConst.BUCKET_CNT;
import static org.example.buffer.JoinConst.PARTITION_CNT;
import static org.example.processor.DMLOrganizer.*;
import static org.example.record.NullConst.NULL_LINK;
import static org.example.record.NullConst.isNullAttribute;

public class QueryEvaluationEngine {
    List<Record> getRecordsForSelectAll(RelationMetadata relationMetadata, List<AttributeMetadata> attributeMetadataList, int recordSize, List<Integer> attPosOfPrimaryKey) throws IOException {
        List<Record> resultSet = new ArrayList<>(); // This result set will save the records to show to user

        // Read from the file
        RandomAccessFile input = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "r");
        int blockIdx = 0;
        while(true) {
            int blockSize = recordSize * BlockingFactor.VAL;

            byte[] readBlockBytes = new byte[blockSize];
            int readCnt = input.read(readBlockBytes);
            Block readBlock = new Block(blockIdx++, readBlockBytes, attributeMetadataList);

            if(readCnt == -1) { // EOF
                break;
            }

            for(int i = 0; i < BlockingFactor.VAL; i++) {
                Record readRecord = readBlock.getRecords()[i];

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

        int blockIdx = 0;
        while(true) {
            int blockSize = recordSize * BlockingFactor.VAL;

            byte[] readBlockBytes = new byte[blockSize];
            int readCnt = input.read(readBlockBytes);
            Block readBlock = new Block(blockIdx++, readBlockBytes, attributeMetadataList);

            if(readCnt == -1) { // EOF
                break;
            }

            for(int i = 0; i < BlockingFactor.VAL; i++) {
                Record readRecord = readBlock.getRecords()[i];

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
            } else { // Next record that header point isn't in header block
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

    JoinedRecords getJoinedRecords(RelationMetadata[] relationMetadataArr, List<AttributeMetadata>[] attributeMetadataListArr, List<String> joinAttrs, HashMap<String, Integer>[] joinAttrPosArr) {
        File[] temporaryFilesR;
        File[] temporaryFilesS;

        RelationMetadata relationMetadataR = relationMetadataArr[0];
        RelationMetadata relationMetadataS = relationMetadataArr[1];
        List<AttributeMetadata> attrMetadataListR = attributeMetadataListArr[0];
        List<AttributeMetadata> attrMetadataListS = attributeMetadataListArr[1];
        List<Integer> attPosOfPrimaryKeyR = getAttPosOfPrimaryKey(attrMetadataListR);
        List<Integer> attPosOfPrimaryKeyS = getAttPosOfPrimaryKey(attrMetadataListS);
        HashMap<String, Integer> joinAttrPosR = joinAttrPosArr[0];
        HashMap<String, Integer> joinAttrPosS = joinAttrPosArr[1];

        // Read from the file
        try {
            BufferPage[] partitioningBuffPages = new BufferPage[PARTITION_CNT];
            BufferPage inputBuffPage = new BufferPage();

            for(int i = 0; i < partitioningBuffPages.length; i++) {
                partitioningBuffPages[i] = new BufferPage();
            }
            // Partitioning relation R
            temporaryFilesR = partition(joinAttrs, attrMetadataListR, relationMetadataR, attPosOfPrimaryKeyR, inputBuffPage, joinAttrPosR, partitioningBuffPages, "r");
            // Partitioning relation S
            temporaryFilesS = partition(joinAttrs, attrMetadataListS, relationMetadataS, attPosOfPrimaryKeyS, inputBuffPage, joinAttrPosS, partitioningBuffPages, "s");

            // Matching records ang get JoinedRecords
            JoinedRecords joinedRecords = getJoinedRecords(joinAttrs, attrMetadataListS, attrMetadataListR, joinAttrPosS, joinAttrPosR, temporaryFilesS, temporaryFilesR);

            // Delete temporary files
            deleteTempFiles(temporaryFilesR, temporaryFilesS);

            return joinedRecords;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static File[] partition(List<String> joinAttrs, List<AttributeMetadata> attrMetadataList, RelationMetadata relationMetadata,
                                    List<Integer> attPosOfPrimaryKey, BufferPage inputBuffPage, HashMap<String, Integer> joinAttrPosMap, BufferPage[] partitioningBuffPages, String tempFileName) throws IOException {
        File[] temporaryFiles;
        int recordSize = getRecordSize(attrMetadataList);
        RandomAccessFile input = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "r");

        // Creating temporary files for Partitioning
        temporaryFiles = new File[PARTITION_CNT];
        FileOutputStream[] partitionOutput = new FileOutputStream[PARTITION_CNT];
        for(int i = 0; i < PARTITION_CNT; i++) {
            temporaryFiles[i] = new File("temporary_file" + "/" + tempFileName + i + ".tmptbl");
            if(temporaryFiles[i].exists()) {
                temporaryFiles[i].delete();
            }
            temporaryFiles[i].createNewFile();
            partitionOutput[i] = new FileOutputStream(temporaryFiles[i], true);
        }

        int blockIdx = 0;
        while(true) {
            int blockSize = recordSize * BlockingFactor.VAL;

            // Read to input buffer page
            byte[] readBlockBytes = new byte[blockSize];
            int readCnt = input.read(readBlockBytes);
            Block readBlock = new Block(blockIdx++, readBlockBytes, attrMetadataList);

            if(readCnt == -1) { // EOF
                break;
            }

            for(int i = 0; i < BlockingFactor.VAL; i++) {
                Record readRecord = readBlock.getRecords()[i];

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
                    inputBuffPage.addRecord(readRecord); // Add valid record to the input buffer page
                }
            }

            // Distribute the records of input buffer page to partitions by hash function
            Record[] records = inputBuffPage.getRecords();
            for(int i = 0; i < inputBuffPage.getRecordCnt(); i++) {
                int joinHashCode = 0;
                Record record = records[i];
                List<char[]> attrList = record.getAttributes();
                for (String attr : joinAttrs) { // Get the XORs of join column of record for partitioning
                    int joinAttrPos = joinAttrPosMap.get(attr);
                    String attrVal = new String(attrList.get(joinAttrPos)).trim();
                    int hashCode = attrVal.hashCode();
                    joinHashCode ^= hashCode;
                }
                // Partitioning by hash function
                int partitionNum = Math.floorMod(joinHashCode, PARTITION_CNT);
                partitioningBuffPages[partitionNum].addRecord(record);

                if(partitioningBuffPages[partitionNum].isFull()) { // If partitionBuffer is full, write to the disk
                    byte[] buffByte = partitioningBuffPages[partitionNum].getByteArray();
                    partitionOutput[partitionNum].write(buffByte);
                    partitioningBuffPages[partitionNum].clear();
                }
            }
            inputBuffPage.clear();
        }

        for(int i = 0; i < partitioningBuffPages.length; i++) { // If partitionBuffer is not empty, write to the disk
            if(partitioningBuffPages[i].getRecordCnt() > 0) {
                byte[] buffByte = partitioningBuffPages[i].getByteArray();
                partitionOutput[i].write(buffByte);
                partitioningBuffPages[i].clear();
            }
        }

        for(int i = 0; i < PARTITION_CNT; i++) {
            partitionOutput[i].close();
        }
        input.close();
        return temporaryFiles;
    }

    private static JoinedRecords getJoinedRecords(List<String> joinAttrs, List<AttributeMetadata> attrMetadataListS, List<AttributeMetadata> attrMetadataListR, HashMap<String, Integer> joinAttrPosS, HashMap<String, Integer> joinAttrPosR, File[] temporaryFilesS, File[] temporaryFilesR) throws IOException {
        RandomAccessFile input;
        List<BufferPage> buildInputBuffPages = new ArrayList<>();
        BufferPage probeInputBuffPage;

        List<AttributeMetadata> attrMetadataListBuild = attrMetadataListS;
        List<AttributeMetadata> attrMetadataListProbe = attrMetadataListR;
        HashMap<String, Integer> joinAttrPosBuild = joinAttrPosS;
        HashMap<String, Integer> joinAttrPosProbe = joinAttrPosR;

        File[] temporaryFilesBuild = temporaryFilesS;
        File[] temporaryFilesProbe = temporaryFilesR;

        List<MatchedRecord> matchedRecords = new ArrayList<>();

        int recordSizeBuild = getRecordSizeExceptLink(attrMetadataListBuild);
        for(int i = 0; i < PARTITION_CNT; i++) {
            // Read BuildInput Partition to the Buffer
            input = new RandomAccessFile(temporaryFilesBuild[i], "r");

            while (true) {
                int blockSize = recordSizeBuild * BlockingFactor.VAL;

                byte[] readBlockBytes = new byte[blockSize];
                int readCnt = input.read(readBlockBytes);
                BufferPage readBlock = new BufferPage(readBlockBytes, attrMetadataListBuild);

                if (readCnt == -1) { // EOF
                    break;
                }

                buildInputBuffPages.add(readBlock);
            }

            // Build hashIndex
            List<Record>[] hashIndex = new ArrayList[BUCKET_CNT];

            for (int key = 0; key < BUCKET_CNT; key++) {
                hashIndex[key] = new ArrayList<>();
            }

            for (BufferPage page : buildInputBuffPages) {
                Record[] records = page.getRecords();
                for (int j = 0; j < page.getRecordCnt(); j++) {
                    Record record = records[j];
                    int joinHashCode = 0;
                    List<char[]> attrList = record.getAttributes();
                    for (String attr : joinAttrs) { // Get the XORs of join column of record for building hash index
                        int joinAttrPos = joinAttrPosBuild.get(attr);
                        String attrVal = new String(attrList.get(joinAttrPos)).trim();
                        int hashCode = attrVal.hashCode();
                        joinHashCode ^= hashCode;
                    }
                    int bucketNum = Math.floorMod(joinHashCode, BUCKET_CNT);
                    List<Record> bucket = hashIndex[bucketNum];
                    bucket.add(record);
                }
            }
            input.close();

            // Reading corresponding Probe input Partition
            input = new RandomAccessFile(temporaryFilesProbe[i], "r");
            int recordSizeProbe = getRecordSizeExceptLink(attrMetadataListProbe);

            while (true) {
                int blockSize = recordSizeProbe * BlockingFactor.VAL;

                byte[] readBlockBytes = new byte[blockSize];
                int readCnt = input.read(readBlockBytes);
                probeInputBuffPage = new BufferPage(readBlockBytes, attrMetadataListProbe);

                if (readCnt == -1) { // EOF
                    break;
                }

                // Probe by using join attributes of probe input
                Record[] records = probeInputBuffPage.getRecords();
                for (int j = 0; j < probeInputBuffPage.getRecordCnt(); j++) {
                    Record record = records[j];
                    int joinHashCode = 0;
                    List<char[]> attrListProbe = record.getAttributes();
                    for (String attr : joinAttrs) { // Get the XORs of join column of record for building hash index
                        int joinAttrPos = joinAttrPosProbe.get(attr);
                        String attrVal = new String(attrListProbe.get(joinAttrPos)).trim();
                        int hashCode = attrVal.hashCode();
                        joinHashCode ^= hashCode;
                    }

                    int bucketNum = Math.floorMod(joinHashCode, BUCKET_CNT);
                    List<Record> bucket = hashIndex[bucketNum];

                    // Matching record of build input partition and record of probe input partition
                    for (Record matchedRecord : bucket) {
                        // Check the join columns are really same between records of probe and build input before concatenating
                        boolean matched = true;
                        List<char[]> attrListBuild = matchedRecord.getAttributes();
                        for (String attr : joinAttrs) {
                            int attrPosBuild = joinAttrPosBuild.get(attr);
                            int attrPosProbe = joinAttrPosProbe.get(attr);

                            String attrValBuild = new String(attrListBuild.get(attrPosBuild)).trim();
                            String attrValProbe = new String(attrListProbe.get(attrPosProbe)).trim();

                            if (!attrValBuild.equals(attrValProbe)) { // Join columns are not same
                                matched = false;
                                break;
                            }
                        }

                        if (matched) { // If two records are matched, concatenate two records
                            matchedRecords.add(new MatchedRecord(record, matchedRecord));
                        }
                    }
                }
            }
            input.close();
            buildInputBuffPages.clear();
        }
        JoinedRecords joinedRecords = new JoinedRecords(attrMetadataListProbe, attrMetadataListBuild, joinAttrs, joinAttrPosBuild, matchedRecords);
        return joinedRecords;
    }

    private static void deleteTempFiles(File[] temporaryFilesR, File[] temporaryFilesS) {
        for(int i = 0; i < PARTITION_CNT; i++) {
            if(temporaryFilesR[i].exists()) {
                temporaryFilesR[i].delete();
                temporaryFilesS[i].delete();
            }
        }
    }
}
