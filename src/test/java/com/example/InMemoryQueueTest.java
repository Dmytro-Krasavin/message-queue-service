package com.example;

import com.example.model.CreateQueueResult;
import com.example.model.Message;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;
import com.example.service.impl.InMemoryQueueService;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public class InMemoryQueueTest {

    public QueueService initQueueService(Duration visibilityTimeout) {
        return new InMemoryQueueService(visibilityTimeout);
    }

    @Test
    public void assertPushPullMessageInQueue() {
        QueueService queueService = initQueueService(Duration.ofMillis(3000));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        String messageBody = "Test message";
        PushMessageResult pushResult = queueService.push(queueUrl, messageBody);
        Message message = queueService.pull(queueUrl);
        assertNotNull(message);
        assertEquals(pushResult.getMessageId(), message.getMessageId());
        assertEquals(message.getBody(), messageBody);
    }

    @Test
    public void assertSuccessDeleteAfterPool() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(10));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        queueService.push(queueUrl, "Test");
        Message message = queueService.pull(queueUrl);
        queueService.delete(queueUrl, message.getReceiptHandle());
        Thread.sleep(20);
        Message afterDeletingMessage = queueService.pull(queueUrl);
        assertNull(afterDeletingMessage);
    }

    @Test
    public void assertMessageBecomeVisibleIfNoDelete() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(1));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        String messageBody = "Test";
        queueService.push(queueUrl, messageBody);
        queueService.pull(queueUrl);
        Thread.sleep(100);
        Message message = queueService.pull(queueUrl);
        assertEquals(message.getBody(), messageBody);
    }

    @Test
    public void assertRemoveFalseIfVisibilityTimeoutOver() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(1));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        queueService.push(queueUrl, "Test");
        Message message = queueService.pull(queueUrl);
        Thread.sleep(100);
        queueService.delete(queueUrl, message.getReceiptHandle());
        assertNotNull(queueService.pull(queueUrl));
    }
}
