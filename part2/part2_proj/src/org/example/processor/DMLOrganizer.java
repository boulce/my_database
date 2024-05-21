package org.example.processor;

import org.example.buffer.BufferPage;
import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.Block;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.example.record.NullConst.NULL_LINK;
import static org.example.record.NullConst.isNullAttribute;

public class DMLOrganizer {
    public boolean isValidRelationMetatdata(Connection conn, String relationName, RelationMetadata relationMetadata) throws SQLException {
        PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM relation_metadata WHERE relation_name = ?");
        pstmt.setString(1, relationName);
        ResultSet rs = pstmt.executeQuery();

        boolean exist = rs.next();
        if (exist) {
            relationMetadata.setRelationName(rs.getString(1));
            relationMetadata.setNumberOfAttributes(rs.getInt(2));
            relationMetadata.setStorageOrganization(rs.getString(3));
            relationMetadata.setLocation(rs.getString(4));
        }

        rs.close();
        pstmt.close();
        return exist;
    }

    public ArrayList<AttributeMetadata> getAttributeMetadataForQuery(Connection conn, RelationMetadata relationMetadata) throws SQLException {
        ArrayList<AttributeMetadata> attributeMetadataList = new ArrayList<>();

        PreparedStatement pstmt = conn.prepareStatement("SELECT * FROM attribute_metadata WHERE relation_name = ? ORDER BY position");
        pstmt.setString(1, relationMetadata.getRelationName());
        ResultSet rs = pstmt.executeQuery();

        while(rs.next()) {
            String relationName = rs.getString(1);
            String attributeName = rs.getString(2);
            String domainType = rs.getString(3);
            int position = rs.getInt(4);
            int length = rs.getInt(5);
            boolean isPrimary = rs.getBoolean(6);
            String referenceRelationName = rs.getString(7);
            String referenceAttributeName = rs.getString(8);
            AttributeMetadata attributeMetadata = new AttributeMetadata(relationName, attributeName, domainType, position, length, isPrimary, referenceRelationName, referenceAttributeName);
            attributeMetadataList.add(attributeMetadata);
        }

        rs.close();
        pstmt.close();
        return attributeMetadataList;
    }

    public List<Record> getResultSetForSelectAll(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList) throws IOException {
        int recordSize = getRecordSize(attributeMetadataList);

        // Get the attribute position of primary key
        // This will be used for judging deleted records and sorting the result set
        List<Integer> attPosOfPrimaryKey = getAttPosOfPrimaryKey(attributeMetadataList);

        QueryEvaluationEngine queryEvaluationEngine = new QueryEvaluationEngine();
        List<Record> resultSet = queryEvaluationEngine.getRecordsForSelectAll(relationMetadata, attributeMetadataList, recordSize, attPosOfPrimaryKey);

        // sort result set by primary key as ascending order
        sortResultSetByAttributes(attPosOfPrimaryKey, resultSet);
        return resultSet;
    }

    public List<Record> getResultSetForSelectOne(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList, Map<Integer, String> primaryKeyMap) throws IOException {
        int recordSize = getRecordSize(attributeMetadataList);

        QueryEvaluationEngine queryEvaluationEngine = new QueryEvaluationEngine();
        List<Record> resultSet = queryEvaluationEngine.getRecordsForSelectOne(relationMetadata, attributeMetadataList, recordSize, primaryKeyMap);

        return resultSet;
    }

    public List<Integer> getAttPosOfPrimaryKey(List<AttributeMetadata> attributeMetadataList) {
        List<Integer> attPosOfPrimaryKey = new ArrayList<>();
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            if(attributeMetadata.isPrimary()) {
                attPosOfPrimaryKey.add(attributeMetadata.getPosition());
            }
        }
        return attPosOfPrimaryKey;
    }

    public void testJoin(RelationMetadata[] relationMetadataArr, List<AttributeMetadata>[] attributeMetadataListArr, List<String> joinAttr, HashMap<String, Integer>[] joinAttrPosArr) {
        final int PARTITION_CNT = 3;

        File[] temporaryFilesR;
        File[] temporaryFilesS;
        ///////////////////////////////////////////////////////////////////// START OF PARTITIONING
        BufferPage[] partitioningBuffPages = new BufferPage[PARTITION_CNT];
        BufferPage inputBuffPage = new BufferPage();

        for(int i = 0; i < partitioningBuffPages.length; i++) {
            partitioningBuffPages[i] = new BufferPage();
        }

        //Read from the file
        try {
            RelationMetadata relationMetadataR = relationMetadataArr[0];
            RelationMetadata relationMetadataS = relationMetadataArr[1];
            List<AttributeMetadata> attrMetadataListR = attributeMetadataListArr[0];
            List<AttributeMetadata> attrMetadataListS = attributeMetadataListArr[1];
            List<Integer> attPosOfPrimaryKeyR = getAttPosOfPrimaryKey(attrMetadataListR);
            List<Integer> attPosOfPrimaryKeyS = getAttPosOfPrimaryKey(attrMetadataListS);
            HashMap<String, Integer> joinAttrPosR = joinAttrPosArr[0];
            HashMap<String, Integer> joinAttrPosS = joinAttrPosArr[1];


            // TODO refactoring partitioning for R, S. There is duplicate code

            // Partitioning relation R
            int recordSize = getRecordSize(attrMetadataListR);
            RandomAccessFile input = new RandomAccessFile(relationMetadataR.getLocation() + relationMetadataR.getRelationName() + ".tbl", "r");

            // Creating temporary files for Partitioning
            temporaryFilesR = new File[PARTITION_CNT];
            FileOutputStream[] partitionInput = new FileOutputStream[PARTITION_CNT];
            for(int i = 0; i < PARTITION_CNT; i++) {
                temporaryFilesR[i] = new File("temporary_file/r" + i + ".tmptbl");
                if(temporaryFilesR[i].exists()) {
                    temporaryFilesR[i].delete();
                }
                temporaryFilesR[i].createNewFile();
                partitionInput[i] = new FileOutputStream(temporaryFilesR[i], true);
            }

            int blockIdx = 0;
            while(true) {
                int blockSize = recordSize * BlockingFactor.VAL;

                byte[] readBlockBytes = new byte[blockSize];
                int readCnt = input.read(readBlockBytes);
                Block readBlock = new Block(blockIdx++, readBlockBytes, attrMetadataListR);

                if(readCnt == -1) { // EOF
                    break;
                }

                for(int i = 0; i < BlockingFactor.VAL; i++) {
                    Record readRecord = readBlock.getRecords()[i];

                    // If some attribute of primary-key of a record is NULL, it is a deleted record.
                    // So don't contain the deleted record to the result set
                    boolean isDeleted = false;
                    for(int pos : attPosOfPrimaryKeyR) {
                        if (isNullAttribute(readRecord.getAttributes().get(pos))){
                            isDeleted = true;
                            break;
                        }
                    }
                    if(!isDeleted) {
                        inputBuffPage.addRecord(readRecord); // Add valid record to the input buffer page
                    }
                }

                Record[] records = inputBuffPage.getRecords();
                for(int i = 0; i < inputBuffPage.getRecordCnt(); i++) {
                    int joinHashCode = 0;
                    Record record = records[i];
                    List<char[]> attrList = record.getAttributes();
                    for (String attr : joinAttr) { // Get the XORs of join column of record for partitioning
                        int jointAttrPos = joinAttrPosR.get(attr);
                        String attrVal = new String(attrList.get(jointAttrPos)).trim();
                        int hashCode = attrVal.hashCode();
                        joinHashCode ^= hashCode;
                    }
                    // Partitioning by hash function
                    int partitionNum = Math.floorMod(joinHashCode, PARTITION_CNT);
                    partitioningBuffPages[partitionNum].addRecord(record);

                    if(partitioningBuffPages[partitionNum].isFull()) { // If partitionBuffer is full, write to the disk
                        byte[] buffByte = partitioningBuffPages[partitionNum].getByteArray();
                        partitionInput[partitionNum].write(buffByte);
                        partitioningBuffPages[partitionNum].clear();
                    }
                }
                inputBuffPage.clear();
            }

            for(int i = 0; i < partitioningBuffPages.length; i++) { // If partitionBuffer is not empty, write to the disk
                if(partitioningBuffPages[i].getRecordCnt() > 0) {
                    byte[] buffByte = partitioningBuffPages[i].getByteArray();
                    partitionInput[i].write(buffByte);
                    partitioningBuffPages[i].clear();
                }
            }

            for(int i = 0; i < PARTITION_CNT; i++) {
                partitionInput[i].close();
            }
            input.close();

            // Partitioning relation S
            recordSize = getRecordSize(attrMetadataListS);
            input = new RandomAccessFile(relationMetadataS.getLocation() + relationMetadataS.getRelationName() + ".tbl", "r");

            // Creating temporary files for Partitioning
            temporaryFilesS = new File[PARTITION_CNT];
            partitionInput = new FileOutputStream[PARTITION_CNT];
            for(int i = 0; i < PARTITION_CNT; i++) {
                temporaryFilesS[i] = new File("temporary_file/s" + i + ".tmptbl");
                if(temporaryFilesS[i].exists()) {
                    temporaryFilesS[i].delete();
                }
                temporaryFilesS[i].createNewFile();
                partitionInput[i] = new FileOutputStream(temporaryFilesS[i], true);
            }

            blockIdx = 0;
            while(true) {
                int blockSize = recordSize * BlockingFactor.VAL;

                byte[] readBlockBytes = new byte[blockSize];
                int readCnt = input.read(readBlockBytes);
                Block readBlock = new Block(blockIdx++, readBlockBytes, attrMetadataListS);

                if(readCnt == -1) { // EOF
                    break;
                }

                for(int i = 0; i < BlockingFactor.VAL; i++) {
                    Record readRecord = readBlock.getRecords()[i];

                    // If some attribute of primary-key of a record is NULL, it is a deleted record.
                    // So don't contain the deleted record to the result set
                    boolean isDeleted = false;
                    for(int pos : attPosOfPrimaryKeyS) {
                        if (isNullAttribute(readRecord.getAttributes().get(pos))){
                            isDeleted = true;
                            break;
                        }
                    }
                    if(!isDeleted) {
                        inputBuffPage.addRecord(readRecord); // Add valid record to the input buffer page
                    }
                }

                Record[] records = inputBuffPage.getRecords();
                for(int i = 0; i < inputBuffPage.getRecordCnt(); i++) {
                    int joinHashCode = 0;
                    Record record = records[i];
                    List<char[]> attrList = record.getAttributes();
                    for (String attr : joinAttr) { // Get the XORs of join column of record for partitioning
                        int jointAttrPos = joinAttrPosS.get(attr);
                        String attrVal = new String(attrList.get(jointAttrPos)).trim();
                        int hashCode = attrVal.hashCode();
                        joinHashCode ^= hashCode;
                    }
                    // Partitioning by hash function
                    int partitionNum = Math.floorMod(joinHashCode, PARTITION_CNT);
                    partitioningBuffPages[partitionNum].addRecord(record);

                    if(partitioningBuffPages[partitionNum].isFull()) { // If partitionBuffer is full, write to the disk
                        byte[] buffByte = partitioningBuffPages[partitionNum].getByteArray();
                        partitionInput[partitionNum].write(buffByte);
                        partitioningBuffPages[partitionNum].clear();
                    }
                }
                inputBuffPage.clear();
            }

            for(int i = 0; i < partitioningBuffPages.length; i++) { // If partitionBuffer is not empty, write to the disk
                if(partitioningBuffPages[i].getRecordCnt() > 0) {
                    byte[] buffByte = partitioningBuffPages[i].getByteArray();
                    partitionInput[i].write(buffByte);
                    partitioningBuffPages[i].clear();
                }
            }

            for(int i = 0; i < PARTITION_CNT; i++) {
                partitionInput[i].close();
            }
            input.close();

            ///////////////////////////////////////////////////////////////////// END OF PARTITIONING

            ///////////////////////////////////////////////////////////////////// START OF JOIN
            List<Record> resultSet = new ArrayList<>();
            List<BufferPage> buildInputBuffPages = new ArrayList<>();
            BufferPage probeInputBuffPages = new BufferPage();

            RelationMetadata relationMetadataBuild = relationMetadataS;
            RelationMetadata relationMetadataProbe = relationMetadataR;
            List<AttributeMetadata> attrMetadataListBuild = attrMetadataListS;
            List<AttributeMetadata> attrMetadataListProbe = attrMetadataListR;
            List<Integer> attPosOfPrimaryKeyBuild = attPosOfPrimaryKeyS;
            List<Integer> attPosOfPrimaryKeyProbe = attPosOfPrimaryKeyR;
            HashMap<String, Integer> joinAttrPosBuild = joinAttrPosS;
            HashMap<String, Integer> joinAttrPosProbe = joinAttrPosR;

            File[] temporaryFilesBuild = temporaryFilesS;
            File[] temporaryFilesProbe = temporaryFilesR;

            recordSize = getRecordSizeExceptLink(attrMetadataListBuild);
            for(int i = 0; i < PARTITION_CNT; i++) {
                // Read BuildInput Partition to the Buffer
                input = new RandomAccessFile(temporaryFilesBuild[i], "r");

                while(true) {
                    int blockSize = recordSize * BlockingFactor.VAL;

                    byte[] readBlockBytes = new byte[blockSize];
                    int readCnt = input.read(readBlockBytes);
                    BufferPage readBlock = new BufferPage(readBlockBytes, attrMetadataListBuild);
                    buildInputBuffPages.add(readBlock);

                    if(readCnt == -1) { // EOF
                        break;
                    }
                }

                // Build hashIndex
                HashMap<Integer, List<Record>> hashIndex = new HashMap<>();
                for (BufferPage page : buildInputBuffPages) {
                    Record[] records = page.getRecords();
                    for (int j = 0; j < page.getRecordCnt(); j++) {
                        Record record = records[j];
                        int joinHashCode = 0;
                        List<char[]> attrList = record.getAttributes();
                        for (String attr : joinAttr) { // Get the XORs of join column of record for building hash index
                            int jointAttrPos = joinAttrPosBuild.get(attr);
                            String attrVal = new String(attrList.get(jointAttrPos)).trim();
                            int hashCode = attrVal.hashCode();
                            joinHashCode ^= hashCode;
                        }
                        boolean isExisting = hashIndex.containsKey(joinHashCode);
                        if(!isExisting) {
                            hashIndex.put(joinHashCode, new ArrayList<>());
                        }
                        List<Record> bucket = hashIndex.get(joinHashCode);
                        bucket.add(record);
                    }
                }

                buildInputBuffPages.clear();
                input.close();
            }




            ///////////////////////////////////////////////////////////////////// END OF JOIN

        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void insertRecord(RelationMetadata relationMetadata, Record recordToInsert, ArrayList<AttributeMetadata> attributeMetadataList) throws IOException {
        QueryEvaluationEngine queryEvaluationEngine = new QueryEvaluationEngine();
        queryEvaluationEngine.processInsertQuery(relationMetadata, recordToInsert, attributeMetadataList);
    }

    public void deleteRecord(RelationMetadata relationMetadata, ArrayList<AttributeMetadata> attributeMetadataList, HashMap<Integer, String> primaryKeyMap) throws IOException {
        QueryEvaluationEngine queryEvaluationEngine = new QueryEvaluationEngine();

        queryEvaluationEngine.processDeleteQuery(relationMetadata, attributeMetadataList, primaryKeyMap);
    }

    static int getRecordSize(List<AttributeMetadata> attributeMetadataList) {
        int recordSize = 0;
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            recordSize += attributeMetadata.getLength();
        }
        recordSize += 4;
        return recordSize;
    }

    static int getRecordSizeExceptLink(List<AttributeMetadata> attributeMetadataList) {
        int recordSize = 0;
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            recordSize += attributeMetadata.getLength();
        }
        return recordSize;
    }

    private static void sortResultSetByAttributes(List<Integer> attPosOfPrimaryKey, List<Record> resultSet) {
        resultSet.sort(new Comparator<Record>() {
            @Override
            public int compare(Record a, Record b) {
                for (Integer pos : attPosOfPrimaryKey) {
                    String valA = new String(a.getAttributes().get(pos));
                    String valB = new String(b.getAttributes().get(pos));

                    if(valA.compareTo(valB) < 0) {
                        return -1;
                    } else if (valA.compareTo(valB) > 0) {
                        return 1;
                    }
                }

                return 0; // It won't occur, because the same there won't be same primary key records
            }
        });
    }
}
