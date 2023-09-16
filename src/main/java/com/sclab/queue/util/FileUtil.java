package com.sclab.queue.util;

import java.io.*;
import java.util.Properties;

public class FileUtil {

    public static Properties readPropertyFile(String propertyFilePath) {
        FileReader reader = null;
        try {
            reader = new FileReader(propertyFilePath);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
        Properties properties = new Properties();
        try {
            properties.load(reader);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }

    public static void appendIntoFile(String filePath, String data) {
        try {
            BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filePath, true));
            bufferedWriter.write(data);
            bufferedWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createNewFile(String filePath) {
        createNewFile(filePath, "");
    }

    public static void createNewFile(String filePath, String data) {
        try {
            FileWriter fileWriter = new FileWriter(filePath);
            fileWriter.write(data);
            fileWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}