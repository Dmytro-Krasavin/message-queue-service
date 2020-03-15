package com.example.model;

import java.util.Objects;

public class CreateQueueResult {

    private final String queueUrl;

    public CreateQueueResult(String queueUrl) {
        this.queueUrl = queueUrl;
    }

    public String getQueueUrl() {
        return queueUrl;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CreateQueueResult that = (CreateQueueResult) o;
        return Objects.equals(queueUrl, that.queueUrl);
    }

    @Override
    public int hashCode() {
        return Objects.hash(queueUrl);
    }
}
