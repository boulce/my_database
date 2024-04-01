import java.util.Scanner;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class Main {
    public static void main(String[] args) {
        System.out.println("Start My Database.......");
        System.out.println();

        // TODO JDBC 커넥션 연결해야할듯
        int command;
        while(true) {
            Scanner scanner = new Scanner(System.in);
            System.out.println("[My Database Menu]");
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

        // TODO JDBC 연결 해제해야함

    }
}