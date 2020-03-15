package com.example.service.impl;

import com.example.model.Message;

import java.time.Duration;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class InMemoryQueueService extends AbstractConcurrentCacheableQueueService {

    private final ConcurrentMap<String, BlockingDeque<Message>> messagesByQueueUrl = new ConcurrentHashMap<>();

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
    protected void removeQueue(String queueUrl) {
        messagesByQueueUrl.remove(queueUrl);
    }
}