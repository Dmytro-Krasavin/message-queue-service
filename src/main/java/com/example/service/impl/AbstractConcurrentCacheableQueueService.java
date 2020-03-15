package com.example.service.impl;

import com.example.model.CreateQueueResult;
import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.example.model.PushMessageResult;
import com.example.service.QueueService;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractConcurrentCacheableQueueService implements QueueService {

    protected final Duration visibilityTimeout;
    protected final ConcurrentMap<String, Cache<String, PullMessageResult>> hiddenMessagesByQueueUrl = new ConcurrentHashMap<>();

    private final Lock lock = new ReentrantLock();

    protected AbstractConcurrentCacheableQueueService(Duration visibilityTimeout) {
        if (visibilityTimeout.isNegative() || visibilityTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid visibility timeout");
        }
        this.visibilityTimeout = visibilityTimeout;
    }

    protected abstract BlockingDeque<Message> readQueue(String queueUrl);

    protected abstract void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue);

    protected abstract void removeQueue(String queueUrl);

    @Override
    public CreateQueueResult createNewQueue() {
        lock.lock();
        try {
            String queueUrl = UUID.randomUUID().toString();
            writeQueue(queueUrl, new LinkedBlockingDeque<>());
            hiddenMessagesByQueueUrl.put(queueUrl, buildMessageCache(queueUrl));
            return new CreateQueueResult(queueUrl);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteQueue(String queueUrl) {
        lock.lock();
        try {
            removeQueue(queueUrl);
            hiddenMessagesByQueueUrl.remove(queueUrl);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PushMessageResult push(String queueUrl, String messageBody) {
        lock.lock();
        try {
            BlockingDeque<Message> messages = readQueue(queueUrl);
            if (messages != null) {
                String messageId = UUID.randomUUID().toString();
                messages.addLast(new Message(messageBody, messageId));
                writeQueue(queueUrl, messages);
                return new PushMessageResult(messageId);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PullMessageResult pull(String queueUrl) {
        lock.lock();
        try {
            BlockingDeque<Message> messages = readQueue(queueUrl);
            Cache<String, PullMessageResult> hiddenMessagesCache = hiddenMessagesByQueueUrl.get(queueUrl);

            if (messages != null && hiddenMessagesCache != null) {
                Message message = messages.pollFirst();
                writeQueue(queueUrl, messages);
                if (message != null) {
                    String receiptHandle = UUID.randomUUID().toString();
                    PullMessageResult pullResult = new PullMessageResult(message, receiptHandle, Instant.now());

                    hiddenMessagesCache.put(receiptHandle, pullResult);
                    hiddenMessagesCache.cleanUp();

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
            Cache<String, PullMessageResult> hiddenMessagesCache = hiddenMessagesByQueueUrl.get(queueUrl);
            if (hiddenMessagesCache != null) {
                hiddenMessagesCache.invalidate(receiptHandle);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Builds cache for hidden messages with specified visibility timeout
     * and listener that restore message after expiration.
     */
    protected Cache<String, PullMessageResult> buildMessageCache(String queueUrl) {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(visibilityTimeout.toNanos(), TimeUnit.NANOSECONDS)
                .removalListener(notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        PullMessageResult pullResult = (PullMessageResult) notification.getValue();
                        if (pullResult != null) {
                            restoreMessage(pullResult.getMessage(), queueUrl);
                        }
                    }
                }).build();
    }

    /**
     * Restores hidden message back to queue after the expiration of the message in the cache.
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
}