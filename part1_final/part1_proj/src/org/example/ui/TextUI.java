package org.example.ui;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.processor.DDLInterpreter;
import org.example.record.Block;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.*;
import java.util.*;

import static org.example.db.Config.*;
import static org.example.record.NullConst.NULL_LINK;
import static org.example.util.ByteUtil.*;

public class TextUI {

    public void run() throws SQLException {
        System.out.println(".___  ___. ____    ____     _______       ___      .___________.     ___      .______        ___           _______. _______ ");
        System.out.println("|   \\/   | \\   \\  /   /    |       \\     /   \\     |           |    /   \\     |   _  \\      /   \\         /       ||   ____|");
        System.out.println("|  \\  /  |  \\   \\/   /     |  .--.  |   /  ^  \\    `---|  |----`   /  ^  \\    |  |_)  |    /  ^  \\       |   (----`|  |__   ");
        System.out.println("|  |\\/|  |   \\_    _/      |  |  |  |  /  /_\\  \\       |  |       /  /_\\  \\   |   _  <    /  /_\\  \\       \\   \\    |   __|  ");
        System.out.println("|  |  |  |     |  |        |  '--'  | /  _____  \\      |  |      /  _____  \\  |  |_)  |  /  _____  \\  .----)   |   |  |____ ");
        System.out.println("|__|  |__|     |__|        |_______/ /__/     \\__\\     |__|     /__/     \\__\\ |______/  /__/     \\__\\ |_______/    |_______|");
        System.out.println();

        Connection conn = null;
        DDLInterpreter ddlInterpreter = new DDLInterpreter();
        try {
            conn = DriverManager.getConnection(DB_URL, DB_USER, DB_PASSWORD);
            System.out.println();

            String command;
            while (true) {
                Scanner scanner = new Scanner(System.in);
                System.out.println("--- 1. Create a table");
                System.out.println("--- 2. Insert a tuple");
                System.out.println("--- 3. Delete a tuple");
                System.out.println("--- 4. Select all tuple");
                System.out.println("--- 5. Select a tuple");
                System.out.println("--- 0. Exit");
                System.out.printf("Enter the command: ");
                command = scanner.next();
                System.out.println();

                if (Objects.equals(command, "1")) {
                    // Enter Relation Metadata
                    RelationMetadata relationMetadata = getRelationMetadata(scanner, conn);

                    // Enter Attribute Metadata
                    List<AttributeMetadata> attributeMetadataList = getAttributeMetadataList(scanner, relationMetadata);

                    ddlInterpreter.createRelation(conn, relationMetadata, attributeMetadataList);

                } else if (Objects.equals(command, "2")) {
                    // Get relation metadata
                    RelationMetadata relationMetadata = new RelationMetadata();
                    while(true) {
                        String relationName;

                        System.out.printf("Enter the relation name to which you insert a tuple: ");
                        relationName = scanner.next();
                        relationName = relationName.toLowerCase();
                        System.out.println();

                        // Check primary key constraint
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

                        if(!exist) {
                            System.out.println("[ERROR] There isn't the relation that has same name '" + relationName + "'. Try again.");
                            System.out.println();
                        } else{
                            break;
                        }
                    }

                    // Get attribute metadata list
                    ArrayList<AttributeMetadata> attributeMetadataList = getAttributeMetadataForQuery(conn, relationMetadata);

                    List<char[]> tuple = new ArrayList<>();
                    // Enter attribute information
                    for (AttributeMetadata attributeMetadata : attributeMetadataList) {
                        String valStr;
                        char[] val = new char[attributeMetadata.getLength()];

                        while(true) {
                            System.out.printf("Enter the value of attribute '" + attributeMetadata.getAttributeName() + "' (type: "+attributeMetadata.getDomainType()+", length: "+attributeMetadata.getLength()+"): ");
                            valStr = scanner.next();
                            System.out.println();

                            if(valStr.length() > attributeMetadata.getLength()) {
                                System.out.println("[ERROR] Your input is bigger than the attribute size. Try again.");
                                System.out.println();
                            }
                            else {
                                break;
                            }
                        }

                        for(int i = 0; i < valStr.length(); i++) {
                            val[i] = valStr.charAt(i);
                        }
                        tuple.add(val);
                    }

                    //TODO 삽입 전에 primary key 존재하는지 판단하고 존재하면 처리해야함, select * from R where VAL = ? 먼저 구현하고 갖다 쓰기

                    Record recordToInsert = new Record(tuple, 0);

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

                } else if (Objects.equals(command, "3")) {

                } else if (Objects.equals(command, "4")) {
                    // Get relation metadata
                    RelationMetadata relationMetadata = getValidMetadata(scanner, conn);

                    // Get attribute metadata list
                    ArrayList<AttributeMetadata> attributeMetadataList = getAttributeMetadataForQuery(conn, relationMetadata);

                    int recordSize = getRecordSize(attributeMetadataList);

                    // Get the attribute position of primary key
                    // This will be used for judging deleted records and sorting the result set
                    ArrayList<Integer> attPosOfPrimaryKey = getAttPosOfPrimaryKey(attributeMetadataList);

                    List<Record> resultSet = getResultSetForSelectAll(relationMetadata, recordSize, attributeMetadataList, attPosOfPrimaryKey);

                    // Print the result set
                    printResultSet(attributeMetadataList, resultSet);

                } else if (Objects.equals(command, "5")) {

                } else if (Objects.equals(command, "0")) {
                    System.out.println("Stop My Database.......");
                    break;
                } else {
                    System.out.println("[ERROR] You input wrong command. Please try again.");
                }
                System.out.println();
            }
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }

    private RelationMetadata getRelationMetadata(Scanner scanner, Connection conn) throws SQLException {
        // Enter Relation Metadata
        String relationName;
        int numberOfAttributes;
        String storageOrganization;
        String location;

        while(true) {
            System.out.println("Enter relation information...");
            System.out.printf("Enter the relation name: ");
            relationName = scanner.next();
            relationName = relationName.toLowerCase();
            System.out.println();

            // Check primary key constraint
            PreparedStatement pstmt = conn.prepareStatement("SELECT relation_name FROM relation_metadata WHERE relation_name = ?");
            pstmt.setString(1, relationName);
            ResultSet rs = pstmt.executeQuery();

            boolean exist = rs.next();
            String existingRelationName = null;
            if (exist) {
                existingRelationName = rs.getString(1);
            }

            pstmt.close();
            rs.close();

            if(exist) {
                System.out.println("[ERROR] There is the relation that has same name '" + existingRelationName+ "'. Try again.");
                System.out.println();
            } else{
                break;
            }
        }

        System.out.printf("Enter the storage number of attributes: ");
        numberOfAttributes = scanner.nextInt();
        System.out.println();

        while(true) {
            System.out.printf("Enter the storage organization: ");
            storageOrganization = scanner.next();
            System.out.println();

            if(!Objects.equals(storageOrganization, "free_list")) {
                System.out.println("[ERROR] Not supported storage organization. Try again.");
                System.out.println();
            } else {
                break;
            }
        }

        while(true) {
            System.out.printf("Enter the location: ");
            location = scanner.next();
            System.out.println();

            if(!Objects.equals(location, "./relation_file/")) {
                System.out.println("[ERROR] Not supported location. Try again.");
                System.out.println();
            } else {
                break;
            }
        }

        return new RelationMetadata(relationName, numberOfAttributes, storageOrganization, location);
    }

    private static ArrayList<AttributeMetadata> getAttributeMetadataList(Scanner scanner, RelationMetadata relationMetadata) {
        String attributeName;
        String domainType;
        int length;
        boolean isPrimary;
        String referenceRelationName;
        String referenceAttributeName;

        ArrayList<AttributeMetadata> attributeMetadataList = new ArrayList<>();
        for(int attIdx = 0; attIdx < relationMetadata.getNumberOfAttributes(); attIdx++) {
            System.out.println("Enter attribute[" + attIdx + "] information...");

            while(true) {
                System.out.printf("Enter the attribute name: ");
                attributeName = scanner.next();
                attributeName = attributeName.toLowerCase();
                System.out.println();

                // Check primary key constraint
                boolean exist = false;
                for (AttributeMetadata data : attributeMetadataList) {
                    if(Objects.equals(data.getAttributeName(), attributeName)) {
                        exist = true;
                        break;
                    }
                }

                if(exist) {
                    System.out.println("[ERROR] There is the attribute that has same name '" + attributeName+ "'. Try again.");
                    System.out.println();
                } else {
                    break;
                }
            }

            while(true) {
                System.out.printf("Enter the domain type: ");
                domainType = scanner.next();
                System.out.println();

                if(!Objects.equals(domainType, "char")) {
                    System.out.println("[ERROR] Not supported domain type. Try again.");
                    System.out.println();
                } else {
                    break;
                }
            }

            System.out.printf("Enter the length: ");
            length = scanner.nextInt();
            System.out.println();


            String isPrimaryStr;
            System.out.printf("Answer whether it is primary key  (if it is true, enter 'yes'): ");
            isPrimaryStr = scanner.next();
            System.out.println();

            if(Objects.equals(isPrimaryStr, "yes")) {
                isPrimary = true;
            } else {
                isPrimary = false;
            }

            while(true){
                System.out.printf("Enter the reference relation name (if you want null, enter 'NULL'): ");
                referenceRelationName = scanner.next();
                System.out.println();

                if(!Objects.equals(referenceRelationName.toUpperCase(), "NULL")) {
                    System.out.println("[ERROR] Not supported reference relation Name. Try again.");
                    System.out.println();
                } else {
                    break;
                }

                if(Objects.equals(referenceRelationName.toUpperCase(), "NULL")) {
                    referenceRelationName = null;
                }
            }

            while(true){
                System.out.printf("Enter the reference attribute name (if you want null, enter 'NULL'): ");
                referenceAttributeName = scanner.next();
                System.out.println();

                if(!Objects.equals(referenceAttributeName.toUpperCase(), "NULL")) {
                    System.out.println("[ERROR] Not supported reference attribute Name. Try again.");
                    System.out.println();
                } else {
                    break;
                }

                if(Objects.equals(referenceAttributeName.toUpperCase(), "NULL")) {
                    referenceRelationName = null;
                }
            }

            AttributeMetadata createAtt = new AttributeMetadata(relationMetadata.getRelationName(), attributeName, domainType, attIdx, length, isPrimary, referenceRelationName, referenceAttributeName);
            attributeMetadataList.add(createAtt);
        }
        return attributeMetadataList;
    }

    private boolean isNullAttribute(char[] attribute) {
        for (char c : attribute) {
            if(c != 0) {
                return false;
            }
        }
        return true;
    }

    private static RelationMetadata getValidMetadata(Scanner scanner, Connection conn) throws SQLException {
        RelationMetadata relationMetadata = new RelationMetadata();
        while(true) {
            String relationName;

            System.out.printf("Enter the relation name from which you select all tuple: ");
            relationName = scanner.next();
            relationName = relationName.toLowerCase();
            System.out.println();

            // Check primary key constraint
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

            if(!exist) {
                System.out.println("[ERROR] There isn't the relation that has same name '" + relationName + "'. Try again.");
                System.out.println();
            } else{
                break;
            }
        }
        return relationMetadata;
    }

    private static ArrayList<AttributeMetadata> getAttributeMetadataForQuery(Connection conn, RelationMetadata relationMetadata) throws SQLException {
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

    private static int getRecordSize(ArrayList<AttributeMetadata> attributeMetadataList) {
        int recordSize = 0;
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            recordSize += attributeMetadata.getLength();
        }
        recordSize += 4;
        return recordSize;
    }

    private static ArrayList<Integer> getAttPosOfPrimaryKey(ArrayList<AttributeMetadata> attributeMetadataList) {
        ArrayList<Integer> attPosOfPrimaryKey = new ArrayList<>();
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            if(attributeMetadata.isPrimary()) {
                attPosOfPrimaryKey.add(attributeMetadata.getPosition());
            }
        }
        return attPosOfPrimaryKey;
    }

    private List<Record> getResultSetForSelectAll(RelationMetadata relationMetadata, int recordSize, ArrayList<AttributeMetadata> attributeMetadataList, ArrayList<Integer> attPosOfPrimaryKey) throws IOException {
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

        // sort result set by primary key as ascending order
        sortResultSetByAttributes(attPosOfPrimaryKey, resultSet);
        return resultSet;
    }

    private static void sortResultSetByAttributes(ArrayList<Integer> attPosOfPrimaryKey, List<Record> resultSet) {
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

    private void printResultSet(ArrayList<AttributeMetadata> attributeMetadataList, List<Record> resultSet) {
        // Draw upper border
        System.out.print("┌");
        for(int s = 0; s < attributeMetadataList.size()-1; s++) {
            for(int i = 0; i < 19; i++) {
                System.out.print("—");
            }
            System.out.print("┬");
        }

        for(int i = 0; i < 19; i++) {
            System.out.print("—");
        }
        System.out.print("┐");

        System.out.println();

        System.out.print("│ ");
        for (AttributeMetadata attributeMetadata : attributeMetadataList) {
            System.out.printf("%20s", attributeMetadata.getAttributeName() + " │ ");
        }
        System.out.println();


        for (Record record : resultSet) {
            // Draw border
            System.out.print("├");
            for(int s = 0; s < attributeMetadataList.size()-1; s++) {
                for(int i = 0; i < 19; i++) {
                    System.out.print("—");
                }
                System.out.print("┼");
            }
            for(int i = 0; i < 19; i++) {
                System.out.print("—");
            }
            System.out.print("┤");
            System.out.println();

            // Draw row
            System.out.print("│ ");
            for (char[] attribute : record.getAttributes()) {
                if(isNullAttribute(attribute)) {
                    System.out.printf("%20s", "null" + " │ ");
                } else {
                    System.out.printf("%20s", new String(attribute).trim() + " │ ");
                }
            }
            System.out.println();
        }

        // Draw lower border
        System.out.print("└");
        for(int s = 0; s < attributeMetadataList.size()-1; s++) {
            for(int i = 0; i < 19; i++) {
                System.out.print("—");
            }
            System.out.print("┴");
        }

        for(int i = 0; i < 19; i++) {
            System.out.print("—");
        }
        System.out.print("┘");
        System.out.println();
    }

}
