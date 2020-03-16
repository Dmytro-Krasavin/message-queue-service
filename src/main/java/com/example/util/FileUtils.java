package com.example.util;

import java.io.*;

public class FileUtils {

    @SuppressWarnings("unchecked")
    public static <T> T readData(File file) {
        try (ObjectInputStream objectInput = new ObjectInputStream(new FileInputStream(file))) {
            return (T) objectInput.readObject();
        } catch (IOException | ClassNotFoundException e) {
            return null;
        }
    }

    public static <T> boolean writeData(File file, T data) {
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(new FileOutputStream(file))) {
            objectOutput.writeObject(data);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
