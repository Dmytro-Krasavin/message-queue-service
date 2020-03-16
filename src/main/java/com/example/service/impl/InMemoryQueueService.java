package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalCause;

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

public class InMemoryQueueService extends AbstractConcurrentCacheableQueueService {

    private final ConcurrentMap<String, BlockingDeque<Message>> messagesByQueueUrl = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Cache<String, PullMessageResult>> hiddenMessagesByQueueUrl = new ConcurrentHashMap<>();

    public InMemoryQueueService(Duration visibilityTimeout) {
        super(visibilityTimeout);
    }

    @Override
    protected BlockingDeque<Message> readQueue(String queueUrl) {
        return messagesByQueueUrl.get(queueUrl);
    }

    @Override
    protected void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue) {
        messagesByQueueUrl.put(queueUrl, messageQueue);
    }

    @Override
    protected Cache<String, PullMessageResult> readCache(String queueUrl) {
        Cache<String, PullMessageResult> messageCache = hiddenMessagesByQueueUrl.get(queueUrl);
        if (messageCache != null) {
            messageCache.cleanUp();
        }
        return messageCache;
    }

    @Override
    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        hiddenMessagesByQueueUrl.put(queueUrl, messageCache);
    }

    /**
     * Builds cache for hidden messages with specified visibility timeout
     * and listener that restore message after visibility timeout expiration.
     */
    @Override
    protected Cache<String, PullMessageResult> buildCache(String queueUrl) {
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
}