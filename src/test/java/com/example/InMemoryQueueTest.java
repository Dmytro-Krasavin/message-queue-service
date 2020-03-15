package com.example;

import com.example.service.QueueService;
import com.example.service.impl.InMemoryQueueService;

import java.time.Duration;

public class InMemoryQueueTest extends AbstractQueueTest {

    public QueueService initQueueService(Duration visibilityTimeout) {
        return new InMemoryQueueService(visibilityTimeout);
    }
}
