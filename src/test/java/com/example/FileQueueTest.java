package com.example;

import com.example.service.QueueService;
import com.example.service.impl.FileQueueService;
import com.example.util.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.time.Duration;

public class FileQueueTest extends AbstractQueueTest {

    private static final String STORAGE_DIR = "test-storage/";

    @After
    public void deleteTestDir() {
        File directory = new File(STORAGE_DIR);
        FileUtils.deleteDirectory(directory);
    }

    @Override
    protected QueueService initQueueService(Duration visibilityTimeout) {
        return new FileQueueService(visibilityTimeout, STORAGE_DIR);
    }
}
