package org.example;

import org.example.metadata.RelationMetadata;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.*;
import java.util.Objects;
import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static String dbURL = "jdbc:mysql://localhost:3306/part1?serverTimezone=UTC&useUniCode=yes&characterEncoding=UTF-8";
    private static String dbUser = "root";
    private static String dbPassword = "0905";


    public static void main(String[] args) throws SQLException {

        System.out.println();
        System.out.println(".___  ___. ____    ____     _______       ___      .___________.     ___      .______        ___           _______. _______ ");
        System.out.println("|   \\/   | \\   \\  /   /    |       \\     /   \\     |           |    /   \\     |   _  \\      /   \\         /       ||   ____|");
        System.out.println("|  \\  /  |  \\   \\/   /     |  .--.  |   /  ^  \\    `---|  |----`   /  ^  \\    |  |_)  |    /  ^  \\       |   (----`|  |__   ");
        System.out.println("|  |\\/|  |   \\_    _/      |  |  |  |  /  /_\\  \\       |  |       /  /_\\  \\   |   _  <    /  /_\\  \\       \\   \\    |   __|  ");
        System.out.println("|  |  |  |     |  |        |  '--'  | /  _____  \\      |  |      /  _____  \\  |  |_)  |  /  _____  \\  .----)   |   |  |____ ");
        System.out.println("|__|  |__|     |__|        |_______/ /__/     \\__\\     |__|     /__/     \\__\\ |______/  /__/     \\__\\ |_______/    |_______|");
        System.out.println();

        Connection conn = null;
        try {
            conn = DriverManager.getConnection(dbURL, dbUser, dbPassword);

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
//                    TODO 테이블 생성, 파일은 table 디렉토리에 저장
                    // Enter Relation Information
                    String relationName;
                    String storageOrganization;
                    String location;
                    int numberOfAttributes;

                    while(true) {
                        System.out.printf("Enter the relation name: ");
                        relationName = scanner.next();
                        System.out.println();

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
                            System.out.println("[ERROR] There are relation that has same name '" + existingRelationName+ "'. Try again");
                            System.out.println();
                        }
                        else{
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
                            System.out.println("[ERROR] Not supported storage organization. Try again");
                            System.out.println();
                        }
                        else {
                            break;
                        }
                    }

                    while(true) {
                        System.out.printf("Enter the location: ");
                        location = scanner.next();
                        System.out.println();

                        if(!Objects.equals(location, "./relation_file")) {
                            System.out.println("[ERROR] Not supported location. Try again");
                            System.out.println();
                        }
                        else {
                            break;
                        }
                    }

                    RelationMetadata relationMetadata = new RelationMetadata(relationName, numberOfAttributes, storageOrganization, location);


                } else if (Objects.equals(command, "2")) {
;
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
        } finally {
            if(conn != null) {
                conn.close();
            }
        }
    }
}