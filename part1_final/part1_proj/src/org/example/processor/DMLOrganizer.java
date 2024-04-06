package org.example.processor;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.Block;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import static org.example.record.NullConst.NULL_LINK;

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

    public void insertRecord(RelationMetadata relationMetadata, Record recordToInsert, ArrayList<AttributeMetadata> attributeMetadataList) throws IOException {
        QueryEvaluationEngine queryEvaluationEngine = new QueryEvaluationEngine();
        queryEvaluationEngine.processInsertQuery(relationMetadata, recordToInsert, attributeMetadataList);
    }

    private static int getRecordSize(ArrayList<AttributeMetadata> attributeMetadataList) {
        int recordSize = 0;
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            recordSize += attributeMetadata.getLength();
        }
        recordSize += 4;
        return recordSize;
    }

    public List<Integer> getAttPosOfPrimaryKey(ArrayList<AttributeMetadata> attributeMetadataList) {
        List<Integer> attPosOfPrimaryKey = new ArrayList<>();
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            if(attributeMetadata.isPrimary()) {
                attPosOfPrimaryKey.add(attributeMetadata.getPosition());
            }
        }
        return attPosOfPrimaryKey;
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
