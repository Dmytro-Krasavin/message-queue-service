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

    private static final String STORAGE_DIR = "test-storage/";

    @After
    public void deleteTestDir() throws IOException {
        File directory = new File(STORAGE_DIR);
        Files.walk(directory.toPath())
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        directory.delete();
    }

    @Override
    protected QueueService initQueueService(Duration visibilityTimeout) {
        return new FileQueueService(visibilityTimeout, STORAGE_DIR);
    }
}
