package org.example.processor;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.record.Block;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DDLInterpreter {
    public static void createRelation(Connection conn, RelationMetadata relationMetadata, List<AttributeMetadata> attributeMetadataList) throws IOException, SQLException {
        // Create new block
        Block nullBlock = getBlock(attributeMetadataList);

        makeRelationFile(relationMetadata.getLocation(), relationMetadata.getRelationName(), nullBlock);

        // Insert relation metadata
        saveRelationMetadata(conn, relationMetadata);

        // Insert attribute metadata
        saveAttribteMetadata(attributeMetadataList, conn);
    }

    private static Block getBlock(List<AttributeMetadata> attributeMetadataList) {
        List<char[]> attChars = getAttChars(attributeMetadataList);
        Block nullBlock = new Block(0, attChars);
        return nullBlock;
    }

    private static void makeRelationFile(String location, String relationName, Block nullBlock) throws IOException {
        RandomAccessFile output = new RandomAccessFile(location + "/" + relationName + ".tbl", "rw");

        output.write(nullBlock.getByteArray()); // // Write record to blockBytes (Block I/O)

        output.close();
    }

    private static void saveRelationMetadata(Connection conn, RelationMetadata relationMetadata) throws SQLException {
        String relInsertSQL = "insert into relation_metadata(relation_name, number_of_attributes, storage_organization, location) values(?,?,?,?)";
        PreparedStatement pstmt = conn.prepareStatement(relInsertSQL);
        pstmt.setString(1, relationMetadata.getRelationName());
        pstmt.setInt(2, relationMetadata.getNumberOfAttributes());
        pstmt.setString(3, relationMetadata.getStorageOrganization());
        pstmt.setString(4, relationMetadata.getLocation());
        pstmt.executeUpdate();
        pstmt.close();
    }

    private static void saveAttribteMetadata(List<AttributeMetadata> attributeMetadataList, Connection conn) throws SQLException {
        String attInsertSQL = "insert into attribute_metadata(relation_name, attribute_name, domain_type, position, length, is_primary, reference_relation_name, reference_attribute_name) values(?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement pstmt = conn.prepareStatement(attInsertSQL);

        for (AttributeMetadata att : attributeMetadataList) {
            pstmt.setString(1, att.getRelationName());
            pstmt.setString(2, att.getAttributeName());
            pstmt.setString(3, att.getDomainType());
            pstmt.setInt(4, att.getPosition());
            pstmt.setInt(5, att.getLength());
            pstmt.setBoolean(6, att.isPrimary());
            pstmt.setString(7, att.getReferenceRelationName());
            pstmt.setString(8, att.getReferenceAttributeName());
            pstmt.executeUpdate();
        }
        pstmt.close();
    }

    private static List<char[]> getAttChars(List<AttributeMetadata> attributeMetadataList) {
        List<char[]> attChars = new ArrayList<>();

        for (AttributeMetadata att : attributeMetadataList) {
            attChars.add(new char[att.getLength()]);
        }
        return attChars;
    }
}
