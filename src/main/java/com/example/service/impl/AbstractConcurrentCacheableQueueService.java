package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;
import com.google.common.cache.Cache;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * This class provides a skeletal implementation of the {@code QueueService}
 * interface, represents messages as {@code BlockingDeque}
 * and hidden messages as {@code Cache}.
 * To implement a service, the programmer should override methods
 * for reading and writing this data structures.
 * <p>
 * The programmer should generally provide a constructor with {@code Duration} argument.
 * <p>
 * This class is supporting full concurrency of retrievals and updates.
 */
public abstract class AbstractConcurrentCacheableQueueService implements QueueService {

    protected final Duration visibilityTimeout;

    private final Lock lock = new ReentrantLock();

    protected AbstractConcurrentCacheableQueueService(Duration visibilityTimeout) {
        if (visibilityTimeout.isNegative() || visibilityTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid visibility timeout");
        }
        this.visibilityTimeout = visibilityTimeout;
    }

    protected abstract BlockingDeque<Message> readQueue(String queueUrl);

    protected abstract void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue);

    protected abstract Cache<String, PullMessageResult> readCache(String queueUrl);

    protected abstract void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache);

    protected abstract Cache<String, PullMessageResult> buildCache(String queueUrl);

    @Override
    public PushMessageResult push(String queueUrl, String messageBody) {
        lock.lock();
        try {
            BlockingDeque<Message> messageQueue = readQueue(queueUrl);
            if (messageQueue == null) {
                messageQueue = createQueue(queueUrl);
            }
            String messageId = UUID.randomUUID().toString();
            messageQueue.addLast(new Message(messageBody, messageId));
            writeQueue(queueUrl, messageQueue);
            return new PushMessageResult(messageId);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PullMessageResult pull(String queueUrl) {
        lock.lock();
        try {
            Cache<String, PullMessageResult> hiddenMessagesCache = readCache(queueUrl);
            BlockingDeque<Message> messages = readQueue(queueUrl);

            if (messages != null && hiddenMessagesCache != null) {
                Message message = messages.pollFirst();
                writeQueue(queueUrl, messages);
                if (message != null) {
                    String receiptHandle = UUID.randomUUID().toString();
                    PullMessageResult pullResult = new PullMessageResult(message, receiptHandle, Instant.now());

                    hiddenMessagesCache.put(receiptHandle, pullResult);
                    writeCache(queueUrl, hiddenMessagesCache);
                    return pullResult;
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void delete(String queueUrl, String receiptHandle) {
        lock.lock();
        try {
            Cache<String, PullMessageResult> hiddenMessagesCache = readCache(queueUrl);
            if (hiddenMessagesCache != null) {
                hiddenMessagesCache.invalidate(receiptHandle);
                writeCache(queueUrl, hiddenMessagesCache);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Restores hidden message back to the queue.
     */
    protected void restoreMessage(Message message, String queueUrl) {
        lock.lock();
        try {
            BlockingDeque<Message> messages = readQueue(queueUrl);
            if (messages != null && message != null) {
                messages.addFirst(message);
                writeQueue(queueUrl, messages);
            }
        } finally {
            lock.unlock();
        }
    }

    private BlockingDeque<Message> createQueue(String queueUrl) {
        BlockingDeque<Message> messageQueue = new LinkedBlockingDeque<>();
        writeQueue(queueUrl, messageQueue);
        writeCache(queueUrl, buildCache(queueUrl));
        return messageQueue;
    }
}