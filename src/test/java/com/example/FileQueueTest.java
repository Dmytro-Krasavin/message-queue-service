package com.example;

import com.example.model.PullMessageResult;
import com.example.service.QueueService;
import com.example.service.impl.FileQueueService;
import com.example.util.FileUtils;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.time.Duration;

import static org.junit.Assert.*;

public class FileQueueTest extends AbstractQueueTest {

    private static final String STORAGE_DIR = "test-storage/";

    @Override
    protected QueueService initQueueService(Duration visibilityTimeout) {
        return new FileQueueService(visibilityTimeout, STORAGE_DIR);
    }

    @After
    public void deleteTestDir() {
        File directory = new File(STORAGE_DIR);
        FileUtils.deleteDirectory(directory);
    }

    @Test
    public void assertServicesHaveCommonPersistenceState() {
        QueueService firstService = initQueueService(Duration.ofMillis(100));
        QueueService secondService = initQueueService(Duration.ofMillis(100));
        String queueUrl = "test-queue-url";
        String message = "message";

        firstService.push(queueUrl, message);
        PullMessageResult pullResult = secondService.pull(queueUrl);

        assertNull(firstService.pull(queueUrl));
        assertNotNull(pullResult);
        assertEquals(message, pullResult.getMessage().getBody());
    }

    @Test
    public void assertServiceRestoreInvisibleMessageAfterRestart() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(100));
        String queueUrl = "test-queue-url";
        String message = "message";

        queueService.push(queueUrl, message);
        queueService.pull(queueUrl);

        assertNull(queueService.pull(queueUrl));

        // simulates service restart and initializes it again
        QueueService restoredQueueService = initQueueService(Duration.ofMillis(100));
        Thread.sleep(200);
        PullMessageResult pullResult = restoredQueueService.pull(queueUrl);

        assertNotNull(pullResult);
        assertEquals(message, pullResult.getMessage().getBody());
    }
}
