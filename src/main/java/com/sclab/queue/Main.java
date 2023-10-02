package com.sclab.queue;

import java.util.Scanner;

public class Main {

    public static void main(String[] args) {
        System.out.print("Hello and welcome to kafka-test-util!");
        System.out.println("Enter [1] for Producer, [2] for Consumer : ");
        Scanner scanner = new Scanner(System.in);
        int option = scanner.nextInt();
        if (option == 2) {
            ConsumerImpl.consumeMessages();
        } else {
            System.out.println("This  option not implemented yet.");
        }
    }
}