package com.example;

import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.*;

public abstract class AbstractQueueTest {

    protected abstract QueueService initQueueService(Duration visibilityTimeout);

    @Test
    public void assertPushAndPullMessageInQueue() {
        QueueService queueService = initQueueService(Duration.ofMillis(3000));
        String queueUrl = "test-queue-url";
        String messageBody = "message";

        PushMessageResult pushResult = queueService.push(queueUrl, messageBody);
        PullMessageResult pullResult = queueService.pull(queueUrl);

        assertNotNull(pullResult);
        assertEquals(pushResult.getMessageId(), pullResult.getMessage().getMessageId());
        assertEquals(messageBody, pullResult.getMessage().getBody());
    }

    @Test
    public void assertCorrectMessagesOrderAfterPull() {
        QueueService queueService = initQueueService(Duration.ofMillis(3000));
        String queueUrl = "test-queue-url";
        String firstMessage = "first";
        String secondMessage = "second";

        PushMessageResult firstPush = queueService.push(queueUrl, firstMessage);
        PushMessageResult secondPush = queueService.push(queueUrl, secondMessage);
        PullMessageResult firstPull = queueService.pull(queueUrl);
        PullMessageResult secondPull = queueService.pull(queueUrl);

        assertNotNull(firstPull);
        assertNotNull(secondPull);
        assertEquals(firstPush.getMessageId(), firstPull.getMessage().getMessageId());
        assertEquals(secondPush.getMessageId(), secondPull.getMessage().getMessageId());
        assertEquals(firstMessage, firstPull.getMessage().getBody());
        assertEquals(secondMessage, secondPull.getMessage().getBody());
    }

    @Test
    public void assertSuccessfulDeleteAfterPull() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(100));
        String queueUrl = "test-queue-url";

        queueService.push(queueUrl, "message");
        PullMessageResult pullResult = queueService.pull(queueUrl);
        queueService.delete(queueUrl, pullResult.getReceiptHandle());
        Thread.sleep(200);
        PullMessageResult afterDeletingPullResult = queueService.pull(queueUrl);

        assertNull(afterDeletingPullResult);
    }

    @Test
    public void assertMessageBecomeInvisibleAfterPull() {
        QueueService queueService = initQueueService(Duration.ofMillis(3000));
        String queueUrl = "test-queue-url";

        queueService.push(queueUrl, "message");
        PullMessageResult firstPull = queueService.pull(queueUrl);
        PullMessageResult secondPull = queueService.pull(queueUrl);

        assertNull(secondPull);
    }

    @Test
    public void assertMessageBecomeVisibleIfItIsNotDeleted() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(1));
        String queueUrl = "test-queue-url";

        String messageBody = "message";
        queueService.push(queueUrl, messageBody);
        PullMessageResult firstPull = queueService.pull(queueUrl);
        Thread.sleep(100);
        PullMessageResult secondPull = queueService.pull(queueUrl);

        assertNotNull(secondPull);
        assertEquals(messageBody, secondPull.getMessage().getBody());
        assertEquals(firstPull.getMessage(), secondPull.getMessage());
    }

    @Test
    public void assertUnsuccessfulDeleteAfterInvisibleTimeoutExpired() throws InterruptedException {
        QueueService queueService = initQueueService(Duration.ofMillis(1));
        String queueUrl = "test-queue-url";

        String messageBody = "message";
        queueService.push(queueUrl, messageBody);
        PullMessageResult firstPull = queueService.pull(queueUrl);
        Thread.sleep(100);

        queueService.delete(queueUrl, firstPull.getReceiptHandle());
        PullMessageResult secondPull = queueService.pull(queueUrl);

        assertNotNull(secondPull);
        assertEquals(messageBody, secondPull.getMessage().getBody());
        assertEquals(firstPull.getMessage(), secondPull.getMessage());
    }
}
