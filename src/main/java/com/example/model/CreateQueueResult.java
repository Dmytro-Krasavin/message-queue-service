package com.example.model;

public class CreateQueueResult {

    private final String queueUrl;

    public CreateQueueResult(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    public String getQueueUrl() {
        return queueUrl;
    }
}
