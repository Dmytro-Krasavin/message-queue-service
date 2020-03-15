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

public class InMemoryQueueService implements QueueService {

    private final Lock lock = new ReentrantLock();

    private final Duration visibilityTimeout;
    private final ConcurrentMap<String, BlockingDeque<Message>> messagesByQueueUrl = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Cache<String, PullMessageResult>> hiddenMessagesByQueueUrl = new ConcurrentHashMap<>();

    public InMemoryQueueService(Duration visibilityTimeout) {
        if (visibilityTimeout.isNegative() || visibilityTimeout.isZero()) {
            throw new IllegalArgumentException("Invalid visibility timeout");
        }
        this.visibilityTimeout = visibilityTimeout;
    }

    @Override
    public CreateQueueResult createNewQueue() {
        lock.lock();
        try {
            String queueUrl = UUID.randomUUID().toString();
            messagesByQueueUrl.put(queueUrl, new LinkedBlockingDeque<>());
            hiddenMessagesByQueueUrl.put(queueUrl, buildMessageCache(visibilityTimeout, queueUrl));
            return new CreateQueueResult(queueUrl);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void deleteQueue(String queueUrl) {
        lock.lock();
        try {
            messagesByQueueUrl.remove(queueUrl);
            hiddenMessagesByQueueUrl.remove(queueUrl);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public PushMessageResult push(String queueUrl, String messageBody) {
        lock.lock();
        try {
            BlockingDeque<Message> messages = messagesByQueueUrl.get(queueUrl);
            if (messages != null) {
                String messageId = UUID.randomUUID().toString();
                messages.addLast(new Message(messageBody, messageId));
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
            BlockingDeque<Message> messages = messagesByQueueUrl.get(queueUrl);
            Cache<String, PullMessageResult> hiddenMessagesCache = hiddenMessagesByQueueUrl.get(queueUrl);

            if (messages != null && hiddenMessagesCache != null) {
                Message message = messages.pollFirst();
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
     * Builds cache for hidden messages with given visibility timeout
     * and listener that restore message after expiration.
     */
    private Cache<String, PullMessageResult> buildMessageCache(Duration visibilityTimeout, String queueUrl) {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(visibilityTimeout.toNanos(), TimeUnit.NANOSECONDS)
                .removalListener(notification -> {
                    if (notification.getCause().equals(RemovalCause.EXPIRED)) {
                        PullMessageResult pullResult = (PullMessageResult) notification.getValue();
                        restoreMessage(pullResult.getMessage(), queueUrl);
                    }
                }).build();
    }

    /**
     * Restores hidden message back to queue after the expiration of the message in the cache.
     */
    private void restoreMessage(Message message, String queueUrl) {
        lock.lock();
        try {
            BlockingDeque<Message> messages = messagesByQueueUrl.get(queueUrl);
            if (messages != null && message != null) {
                messages.addFirst(message);
            }
        } finally {
            lock.unlock();
        }
    }
}