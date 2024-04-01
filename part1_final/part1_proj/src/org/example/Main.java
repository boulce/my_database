package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    private static String dbURL = "jdbc:mysql://localhost:3306/part1?serverTimezone=UTC&useUniCode=yes&characterEncoding=UTF-8";
    private static String dbUser = "root";
    private static String dbPassword = "0905";


    public static void main(String[] args) throws ClassNotFoundException, SQLException {

        System.out.println();
        System.out.println(".___  ___. ____    ____     _______       ___      .___________.     ___      .______        ___           _______. _______ ");
        System.out.println("|   \\/   | \\   \\  /   /    |       \\     /   \\     |           |    /   \\     |   _  \\      /   \\         /       ||   ____|");
        System.out.println("|  \\  /  |  \\   \\/   /     |  .--.  |   /  ^  \\    `---|  |----`   /  ^  \\    |  |_)  |    /  ^  \\       |   (----`|  |__   ");
        System.out.println("|  |\\/|  |   \\_    _/      |  |  |  |  /  /_\\  \\       |  |       /  /_\\  \\   |   _  <    /  /_\\  \\       \\   \\    |   __|  ");
        System.out.println("|  |  |  |     |  |        |  '--'  | /  _____  \\      |  |      /  _____  \\  |  |_)  |  /  _____  \\  .----)   |   |  |____ ");
        System.out.println("|__|  |__|     |__|        |_______/ /__/     \\__\\     |__|     /__/     \\__\\ |______/  /__/     \\__\\ |_______/    |_______|");
        System.out.println();
        Connection conn = DriverManager.getConnection(dbURL, dbUser, dbPassword);

        int command;
        while(true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("--- 1. Create a table");
            System.out.println("--- 2. Insert a tuple");
            System.out.println("--- 3. Delete a tuple");
            System.out.println("--- 4. Select all tuple");
            System.out.println("--- 5. Select a tuple");
            System.out.println("--- 0. Exit");
            System.out.printf("Enter the command: ");
            command = scanner.nextInt();
            System.out.println();

            if(command == 1) {

            } else if(command == 2) {

            } else if(command == 3) {

            } else if(command == 4) {

            } else if(command == 5) {

            } else if(command == 0) {
                System.out.println("Stop My Database.......");
                break;
            } else {
                System.out.println("You input wrong command. Please try again.");
            }
            System.out.println();
        }

        conn.close();
    }
}