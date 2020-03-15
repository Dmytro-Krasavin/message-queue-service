package com.example;

import com.example.service.QueueService;
import com.example.service.impl.FileQueueService;
import org.junit.After;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Comparator;

public class FileQueueTest extends AbstractQueueTest {

    private static final String STORAGE_PATH = "test/";

    @After
    public void deleteTestDir() throws IOException {
        File directory = new File(STORAGE_PATH);
        Files.walk(directory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        boolean delete = directory.delete();
        System.out.println(delete);
    }

    @Override
    protected QueueService initQueueService(Duration visibilityTimeout) {
        return new FileQueueService(visibilityTimeout, STORAGE_PATH);
    }
}
