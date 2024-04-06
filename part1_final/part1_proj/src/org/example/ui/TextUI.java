package org.example.ui;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.processor.DDLInterpreter;
import org.example.processor.DMLOrganizer;
import org.example.record.Block;
import org.example.record.BlockingFactor;
import org.example.record.Record;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

import static org.example.db.Config.*;
import static org.example.record.NullConst.NULL_LINK;
import static org.example.record.NullConst.isNullAttribute;

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
        DMLOrganizer dmlOrganizer = new DMLOrganizer();
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
                    //TODO insert refactoring

                    // Get relation metadata
                    RelationMetadata relationMetadata = new RelationMetadata();
                    while(true) {
                        String relationName;

                        System.out.printf("Enter the relation name to which you insert a tuple: ");
                        relationName = scanner.next();
                        relationName = relationName.toLowerCase();
                        System.out.println();

                        // Check primary key constraint
                        boolean exist = dmlOrganizer.isValidRelationMetatdata(conn, relationName, relationMetadata);

                        if(!exist) {
                            System.out.println("[ERROR] There isn't the relation that has same name '" + relationName + "'. Try again.");
                            System.out.println();
                        } else{
                            break;
                        }
                    }

                    // Get attribute metadata list
                    ArrayList<AttributeMetadata> attributeMetadataList = dmlOrganizer.getAttributeMetadataForQuery(conn, relationMetadata);

                    // Get the attribute position of primary key
                    List<Integer> attPosOfPrimaryKey = dmlOrganizer.getAttPosOfPrimaryKey(attributeMetadataList);

                    // Enter attribute information
                    List<char[]> tuple = null;
                    while(true) {
                        tuple = new ArrayList<>();
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

                        // Check the primary key constraint
                        List<char[]> finalTuple = tuple;
                        Map<Integer, String> primaryKeyMap = attPosOfPrimaryKey.stream().collect(Collectors.toMap(pos -> pos, pos -> new String(finalTuple.get(pos)).trim()));
                        List<Record> resultSet = dmlOrganizer.getResultSetForSelectOne(relationMetadata, attributeMetadataList, primaryKeyMap);

                        if(!resultSet.isEmpty()) {
                            System.out.println("[ERROR] You entered duplicated primary key. Please try again");
                        } else {
                            break;
                        }
                        System.out.println();
                    }

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
                    RelationMetadata relationMetadata = getValidRelationMetadata(scanner, conn, dmlOrganizer);

                    // Get attribute metadata list
                    ArrayList<AttributeMetadata> attributeMetadataList = dmlOrganizer.getAttributeMetadataForQuery(conn, relationMetadata);

                    List<Record> resultSet = dmlOrganizer.getResultSetForSelectAll(relationMetadata, attributeMetadataList);

                    // Print the result set
                    printResultSet(attributeMetadataList, resultSet);

                } else if (Objects.equals(command, "5")) {
                    // Get relation metadata
                    RelationMetadata relationMetadata = getValidRelationMetadata(scanner, conn, dmlOrganizer);

                    // Get attribute metadata list
                    ArrayList<AttributeMetadata> attributeMetadataList = dmlOrganizer.getAttributeMetadataForQuery(conn, relationMetadata);

                    // Get the attribute position of primary key
                    List<Integer> attPosOfPrimaryKey = dmlOrganizer.getAttPosOfPrimaryKey(attributeMetadataList);

                    HashMap<Integer, String> primaryKeyMap = getPrimaryKeyMap(attPosOfPrimaryKey, attributeMetadataList, scanner);

                    List<Record> resultSetForSelectOne = dmlOrganizer.getResultSetForSelectOne(relationMetadata, attributeMetadataList, primaryKeyMap);

                    printResultSet(attributeMetadataList, resultSetForSelectOne);

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

    private static RelationMetadata getValidRelationMetadata(Scanner scanner, Connection conn, DMLOrganizer dmlOrganizer) throws SQLException {
        RelationMetadata relationMetadata = new RelationMetadata();
        while (true) {
            String relationName;

            System.out.printf("Enter the relation name from which you select all tuple: ");
            relationName = scanner.next();
            relationName = relationName.toLowerCase();
            System.out.println();

            // Check primary key constraint
            boolean exist = dmlOrganizer.isValidRelationMetatdata(conn, relationName, relationMetadata);

            if (!exist) {
                System.out.println("[ERROR] There isn't the relation that has same name '" + relationName + "'. Try again.");
                System.out.println();
            } else {
                break;
            }
        }
        return relationMetadata;
    }

    private static HashMap<Integer, String> getPrimaryKeyMap(List<Integer> attPosOfPrimaryKey, ArrayList<AttributeMetadata> attributeMetadataList, Scanner scanner) {
        HashMap<Integer, String> primaryKeyMap = new HashMap<>(); // { primary_key_attribute, value }

        for (Integer pos : attPosOfPrimaryKey) {
            while(true) {
                System.out.printf("Enter the search key of primary key attribute '" + attributeMetadataList.get(pos).getAttributeName() + "' (type: "+ attributeMetadataList.get(pos).getDomainType()+", length: " + attributeMetadataList.get(pos).getLength()+"): ");
                String key = scanner.next();
                System.out.println();

                if(key.length() > attributeMetadataList.get(pos).getLength()) {
                    System.out.println("[ERROR] Your input is bigger than the attribute size. Try again.");
                    System.out.println();
                }
                else {
                    primaryKeyMap.put(pos, key);
                    break;
                }
            }
        }
        return primaryKeyMap;
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
