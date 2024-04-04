package org.example.ui;

import org.example.metadata.AttributeMetadata;
import org.example.metadata.RelationMetadata;
import org.example.processor.DDLInterpreter;
import org.example.record.BlockingFactor;
import org.example.record.Record;
import org.example.util.ByteUtil;

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

                    //TODO 삽입 전에 primary key 존재하는지 판단하고 존재하면 처리해야함

                    Record recordToInsert = new Record(tuple, 0);


                    // TODO free_list 헤더에 넣기 or 새로운 블록 삽입해서 넣기 로직 구현해야함
                    RandomAccessFile input = new RandomAccessFile(relationMetadata.getLocation() + relationMetadata.getRelationName() + ".tbl", "r");
                    int recordSize = recordToInsert.getSize();
                    int blockSize = recordSize * BlockingFactor.VAL;
                    int curReadBlock = 0;
                    byte[] readBlockBytes = new byte[blockSize];
                    input.read(readBlockBytes);
                    input.close();

                    // Get header link
                    byte[] headerLinkBytes = new byte[4];
                    for(int i = recordSize-4, j = 0; i < recordSize; i++, j++) {
                        headerLinkBytes[j] = readBlockBytes[i];
                    }
                    int headerLink = byteArrayToInt(headerLinkBytes);

                    if(headerLink == NULL_LINK) {

                    } else {
                        int nextBlockSeq = headerLink / blockSize;
                        // TODO 디버깅위해 select all 먼저 구현하기
                        // 파일 읽는다
                        // 해당 레코드 위치 찾는다
                        // 헤더의 링크에 해당 레코드의 링크를 대입한다
                        // 해당 레코드에 입력한 레코드를 삽입한다.
                        // 해당 블럭과 헤더 블럭을 파일에 쓴다. (만약 해당 블럭이 헤더 블럭이면 헤더 블럭만 쓴다)
                    }

                      // reading records except header
//                    for(int i = 2*recordSize-4, j = 0; i < 2*recordSize; i++, j++) {
//                        linkBytes[j] = readBlockBytes[i];
//                    }
//                    link = byteArrayToInt(linkBytes);
//                    System.out.println(link);
//
//                    for(int i = 3*recordSize-4, j = 0; i < 3*recordSize; i++, j++) {
//                        linkBytes[j] = readBlockBytes[i];
//                    }
//                    link = byteArrayToInt(linkBytes);
//                    System.out.println(link);


                } else if (Objects.equals(command, "3")) {

                } else if (Objects.equals(command, "4")) {

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
}