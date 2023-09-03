package com.sclab.queue;

public class Producer {

    public static void main(String[] args) {
        String testData = "";
        publishMessage(testData);
    }

    public static void publishMessage(String testData){
        System.out.println("publishing message ...");
    }
}