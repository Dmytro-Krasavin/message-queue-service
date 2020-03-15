package com.example;

import com.example.model.CreateQueueResult;
import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public abstract class AbstractQueueTest {

    protected abstract QueueService initQueueService(Duration visibilityTimeout);

    @Test
    public void assertPushPullMessageInQueue() {
        QueueService queueService = initQueueService(Duration.ofMillis(3000));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        String messageBody = "Test message";
        PushMessageResult pushResult = queueService.push(queueUrl, messageBody);
        PullMessageResult pullResult = queueService.pull(queueUrl);
        assertNotNull(pullResult);
        assertEquals(pushResult.getMessageId(), pullResult.getMessage().getMessageId());
        assertEquals(pullResult.getMessage().getBody(), messageBody);
    }

    @Test
    public void assertSuccessDeleteAfterPool() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(10));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        queueService.push(queueUrl, "Test");
        PullMessageResult pullResult = queueService.pull(queueUrl);
        queueService.delete(queueUrl, pullResult.getReceiptHandle());
        Thread.sleep(20);
        PullMessageResult afterDeletingPullResult = queueService.pull(queueUrl);
        assertNull(afterDeletingPullResult);
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
        PullMessageResult pullResult = queueService.pull(queueUrl);
        assertEquals(pullResult.getMessage().getBody(), messageBody);
    }

    @Test
    public void assertRemoveFalseIfVisibilityTimeoutOver() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(1));
        CreateQueueResult createQueueResult = queueService.createNewQueue();
        String queueUrl = createQueueResult.getQueueUrl();

        queueService.push(queueUrl, "Test");
        PullMessageResult pullResult = queueService.pull(queueUrl);
        Thread.sleep(100);
        queueService.delete(queueUrl, pullResult.getReceiptHandle());
        assertNotNull(queueService.pull(queueUrl));
    }
}
