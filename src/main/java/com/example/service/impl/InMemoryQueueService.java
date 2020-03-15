package com.example.service.impl;

import com.example.model.Message;
import com.example.model.PullMessageResult;
import com.google.common.cache.Cache;

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryQueueService extends AbstractConcurrentCacheableQueueService {

    private final ConcurrentMap<String, BlockingDeque<Message>> messagesByQueueUrl = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, Cache<String, PullMessageResult>> hiddenMessagesByQueueUrl = new ConcurrentHashMap<>();

    public InMemoryQueueService(Duration visibilityTimeout) {
        super(visibilityTimeout);
    }

    @Override
    protected void writeQueue(String queueUrl, BlockingDeque<Message> messageQueue) {
        messagesByQueueUrl.put(queueUrl, messageQueue);
    }

    @Override
    protected void writeCache(String queueUrl, Cache<String, PullMessageResult> messageCache) {
        hiddenMessagesByQueueUrl.put(queueUrl, messageCache);
    }

    @Override
    protected BlockingDeque<Message> readQueue(String queueUrl) {
        return messagesByQueueUrl.get(queueUrl);
    }

    @Override
    protected Cache<String, PullMessageResult> readCache(String queueUrl) {
        return hiddenMessagesByQueueUrl.get(queueUrl);
    }

    @Override
    protected void removeQueue(String queueUrl) {
        messagesByQueueUrl.remove(queueUrl);
    }

    @Override
    protected void removeCache(String queueUrl) {
        hiddenMessagesByQueueUrl.remove(queueUrl);
    }
}